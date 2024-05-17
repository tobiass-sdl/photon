package de.komoot.photon.opensearch;

import de.komoot.photon.*;
import de.komoot.photon.searcher.StructuredSearchHandler;
import de.komoot.photon.searcher.PhotonResult;
import de.komoot.photon.query.StructuredPhotonRequest;
import org.opensearch.client.json.jackson.JacksonJsonpGenerator;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SearchType;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.explain.ExplanationDetail;
import org.opensearch.common.util.iterable.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

/**
 * Execute a structured forward lookup on an Elasticsearch database.
 */
public class OpenSearchStructuredSearchHandler implements StructuredSearchHandler {
    private final OpenSearchClient client;
    private final String[] supportedLanguages;
    private final String queryTimeout;

    public OpenSearchStructuredSearchHandler(OpenSearchClient client, String[] languages, int queryTimeoutSec) {
        this.client = client;
        this.supportedLanguages = languages;
        queryTimeout = queryTimeoutSec + "s";
    }
    @Override
    public List<PhotonResult> search(StructuredPhotonRequest photonRequest) { //TODO deduplicate!!!
        var queryBuilder = buildQuery(photonRequest, false);

        // for the case of deduplication we need a bit more results, #300
        int limit = photonRequest.getLimit();
        int extLimit = limit > 1 ? (int) Math.round(photonRequest.getLimit() * 1.5) : 1;

        var results = sendQuery(queryBuilder.buildQuery(), extLimit);

        if (results.hits().total().value() == 0) {
            System.out.println("****************lenient****************");
            results = sendQuery(buildQuery(photonRequest, true).buildQuery(), extLimit);
        }

        List<PhotonResult> ret = new ArrayList<>();
        for (var hit : results.hits().hits()) {
         /*   System.out.println(hit.explanation().description() + " " + hit.explanation().value());
            for(var x : hit.explanation().details())
            {
                PrintExplanation(x, 1);
            }*/

            System.out.println(hit.matchedQueries());
            ret.add(hit.source().setScore(hit.score()));
        }

        return ret;
    }

    private void PrintExplanation(ExplanationDetail detail, int depth)
    {
        var offset = " ".repeat(depth);
        System.out.println(offset + detail.description()+ " " + detail.value());
        for(var x : detail.details())
        {
            PrintExplanation(x, depth + 1);
        }
    }

    public SearchQueryBuilder buildQuery(StructuredPhotonRequest photonRequest, boolean lenient) {
        return new SearchQueryBuilder(photonRequest, photonRequest.getLanguage(), supportedLanguages, lenient).
                withOsmTagFilters(photonRequest.getOsmTagFilters()).
                withLayerFilters(photonRequest.getLayerFilters()).
                withLocationBias(photonRequest.getLocationForBias(), photonRequest.getScaleForBias(), photonRequest.getZoomForBias()).
                withBoundingBox(photonRequest.getBbox());
    }

    private SearchResponse<OpenSearchResult> sendQuery(Query query, Integer limit) {
        try {
            return client.search(s -> s
                    .index(PhotonIndex.NAME)
                    .searchType(SearchType.QueryThenFetch)
                    .query(query)
                    .size(limit)
                    .explain(true)
                    .timeout(queryTimeout), OpenSearchResult.class);
        } catch (IOException e) {
            throw new RuntimeException("IO error during search", e);
        }
    }
}
