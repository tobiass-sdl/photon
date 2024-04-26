package de.komoot.photon.elasticsearch;

import de.komoot.photon.query.StructuredPhotonRequest;
import de.komoot.photon.searcher.PhotonResult;
import de.komoot.photon.searcher.StructuredSearchHandler;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

/**
 * Execute a structured forward lookup on an Elasticsearch database.
 */
public class ElasticsearchStructuredSearchHandler implements StructuredSearchHandler {
    private final Client client;
    private final String[] supportedLanguages;
    private TimeValue queryTimeout;

    public ElasticsearchStructuredSearchHandler(Client client, String[] languages, int queryTimeoutSec) {
        this.client = client;
        this.supportedLanguages = languages;
        queryTimeout = TimeValue.timeValueSeconds(queryTimeoutSec);
    }


    @Override
    public List<PhotonResult> search(StructuredPhotonRequest photonRequest) { //TODO deduplicate!!!
        PhotonQueryBuilder queryBuilder = buildQuery(photonRequest, false);

        // for the case of deduplication we need a bit more results, #300
        int limit = photonRequest.getLimit();
        int extLimit = limit > 1 ? (int) Math.round(photonRequest.getLimit() * 1.5) : 1;

        SearchResponse results = sendQuery(queryBuilder.buildQuery(), extLimit);

        if (results.getHits().getTotalHits() == 0) {
            results = sendQuery(buildQuery(photonRequest, true).buildQuery(), extLimit);
        }

        List<PhotonResult> ret = new ArrayList<>((int) results.getHits().getTotalHits());
        for (SearchHit hit : results.getHits()) {
            ret.add(new ElasticResult(hit));
        }

        return ret;
    }

    public PhotonQueryBuilder buildQuery(StructuredPhotonRequest photonRequest, boolean lenient) {
        return PhotonQueryBuilder.
                builder(photonRequest, photonRequest.getLanguage(), supportedLanguages, lenient).
                withOsmTagFilters(photonRequest.getOsmTagFilters()).
                withLayerFilters(photonRequest.getLayerFilters()).
                withLocationBias(photonRequest.getLocationForBias(), photonRequest.getScaleForBias(), photonRequest.getZoomForBias()).
                withBoundingBox(photonRequest.getBbox());
    }

    private SearchResponse sendQuery(QueryBuilder queryBuilder, Integer limit) {
        return client.prepareSearch(PhotonIndex.NAME).
                setSearchType(SearchType.QUERY_THEN_FETCH).
                setQuery(queryBuilder).
                setSize(limit).
               // setExplain(true).
                setTimeout(queryTimeout).
                execute().
                actionGet();
    }
}
