package de.komoot.photon.elasticsearch;

import de.komoot.photon.Constants;
import de.komoot.photon.StructuredPhotonRequest;
import de.komoot.photon.query.PhotonRequest;
import de.komoot.photon.searcher.PhotonResult;
import de.komoot.photon.searcher.SearchHandler;
import de.komoot.photon.searcher.StructuredSearchHandler;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

/**
 * Execute a forward lookup on a Elasticsearch database.
 */
public class ElasticsearchStructuredSearchHandler implements StructuredSearchHandler {
    private final Client client;
    private final String[] supportedLanguages;
  //  private boolean lastLenient = false;
    private TimeValue queryTimeout;

    public ElasticsearchStructuredSearchHandler(Client client, String[] languages, int queryTimeoutSec) {
        this.client = client;
        this.supportedLanguages = languages;
        queryTimeout = TimeValue.timeValueSeconds(queryTimeoutSec);
    }

    private BoolQueryBuilder AddFuzzyMustQuery(BoolQueryBuilder query, String name, String value, float boost)
    {
        if(value == null || value.isEmpty()) return query;

        return query.must(QueryBuilders.fuzzyQuery(name, value).boost(boost));
    }

    private BoolQueryBuilder AddTermMustQuery(BoolQueryBuilder query, String name, String value, float boost)
    {
        if(value == null || value.isEmpty()) return query;

        return query.must(QueryBuilders.termQuery(name, value).boost(boost));
    }

    private BoolQueryBuilder AddFuzzyMustQuery(BoolQueryBuilder query, String name, String value, String[] postfixes, float boost)
    {
        if(value == null || value.isEmpty()) return query;

        if(postfixes.length == 1)
        {
            return AddFuzzyMustQuery(query, name + '.' + postfixes[0], value, boost);
        }

        return query.must(GetFuzzyQuery(name, value, postfixes)).boost(boost);
    }

    private BoolQueryBuilder GetFuzzyQuery(String name, String value, String[] postfixes)
    {
        BoolQueryBuilder or = QueryBuilders.boolQuery();
        for(String postfix : postfixes)
        {
            or = or.should(QueryBuilders.fuzzyQuery(name + '.' + postfix, value));
        }

        return or;
    }

    @Override
    public List<PhotonResult> search(StructuredPhotonRequest photonRequest) {

        BoolQueryBuilder query = QueryBuilders.boolQuery();
        query = AddTermMustQuery(query, Constants.COUNTRYCODE, photonRequest.getCountryCode(), 10f);
        String language = photonRequest.getLanguage();
        boolean hasLanguage = language != null && !language.isBlank();
        String[] postfixes = hasLanguage ? new String[] { "default", language } : new String[] { "default" };

        query = AddFuzzyMustQuery(query, Constants.STATE, photonRequest.getState(), postfixes, 2.0f);
        query = AddFuzzyMustQuery(query, Constants.COUNTY, photonRequest.getCounty(), postfixes,1.0f);
        // query = AddMustTermQuery(query, Constants.NAME+ ".default", photonRequest.getCity(), 2.0f); //TODO if no street than go for name?
        String street = photonRequest.getStreet();
        String district = photonRequest.getDistrict();
        String houseNumber = photonRequest.getHouseNumber();
        boolean hasStreetLevelAddress = (street != null && !street.isEmpty()) || (houseNumber != null && !houseNumber.isEmpty());
        boolean hasDistrict = district != null && !district.isEmpty();
        if(hasStreetLevelAddress)
        {
            query = AddFuzzyMustQuery(query, Constants.CITY, photonRequest.getCity(), postfixes,2.0f);
        }
        else {
            BoolQueryBuilder cityFieldQuery = GetFuzzyQuery(Constants.CITY, photonRequest.getCity(), postfixes);
            BoolQueryBuilder nameFieldQuery = GetFuzzyQuery(Constants.NAME, photonRequest.getCity(), postfixes);
            BoolQueryBuilder nameQuery = QueryBuilders.boolQuery().should(cityFieldQuery)
                    .should(nameFieldQuery.filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "city")));

            if(!hasDistrict)
            {
                // prefer cities to districts
                nameQuery = nameQuery.should(GetFuzzyQuery(Constants.NAME, photonRequest.getCity(), postfixes).filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "district")).boost(0.99f));
            }

            query = query.must(nameQuery.boost(2.0f));
        }

        query = AddFuzzyMustQuery(query, Constants.POSTCODE, photonRequest.getPostCode(),2.0f);
        if(hasDistrict)
        {
            if(hasStreetLevelAddress) {
                query = AddFuzzyMustQuery(query, Constants.DISTRICT, district, postfixes,1.0f);
            }
            else {
                BoolQueryBuilder cityFieldQuery = GetFuzzyQuery(Constants.DISTRICT, district, postfixes);
                BoolQueryBuilder nameFieldQuery = GetFuzzyQuery(Constants.NAME, district, postfixes);
                BoolQueryBuilder nameQuery = QueryBuilders.boolQuery().should(cityFieldQuery)
                        .should(nameFieldQuery.filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "district")));
                query = query.must(nameQuery.boost(1.0f));
            }
        }

         //TODO name stuff
        query = AddFuzzyMustQuery(query, Constants.STREET, street,postfixes, 1.0f);

        if(houseNumber != null && houseNumber.isEmpty()) {
            query = AddFuzzyMustQuery(query, Constants.HOUSENUMBER, photonRequest.getHouseNumber(), postfixes,1.0f);
        }
        else {
            query = query.filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("housenumber")));
        }

        QueryBuilder scoredQuery = QueryBuilders.functionScoreQuery(query, new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.linearDecayFunction("importance", "1.0", "0.6")),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.matchQuery("classification", query), ScoreFunctionBuilders.weightFactorFunction(0.1f))
        }).scoreMode(FiltersFunctionScoreQuery.ScoreMode.SUM);

        System.out.println(scoredQuery.toString());
        // for the case of deduplication we need a bit more results, #300
        int limit = photonRequest.getLimit();
        int extLimit = limit > 1 ? (int) Math.round(photonRequest.getLimit() * 1.5) : 1;
//System.out.println("extlimit=" + extLimit);
        SearchResponse results = sendQuery(scoredQuery, extLimit);

        if(results.getHits().getTotalHits() == 0 && hasStreetLevelAddress)
        {
            //TODO try again without the street / house number
        }

        List<PhotonResult> ret = new ArrayList<>((int) results.getHits().getTotalHits());
        for (SearchHit hit : results.getHits()) {
            System.out.println();
            System.out.println(hit.getSource());
            System.out.println(hit.getSource().get(Constants.STREET) + " \t: " + hit.getScore());
            System.out.println(hit.getExplanation().getDescription());
            System.out.println(hit.getExplanation().toString());
            ret.add(new ElasticResult(hit));
        }

        return ret;
    }

   /* public String dumpQuery(PhotonRequest photonRequest) {
        return buildQuery(photonRequest, lastLenient).buildQuery().toString();
    }*/

 /*   public PhotonQueryBuilder buildQuery(StructuredPhotonRequest photonRequest, boolean lenient) {
        lastLenient = lenient;
        return PhotonQueryBuilder.
                builder(photonRequest.getQuery(), photonRequest.getLanguage(), supportedLanguages, lenient).
                withOsmTagFilters(photonRequest.getOsmTagFilters()).
                withLayerFilters(photonRequest.getLayerFilters()).
                withLocationBias(photonRequest.getLocationForBias(), photonRequest.getScaleForBias(), photonRequest.getZoomForBias()).
                withBoundingBox(photonRequest.getBbox());
    }*/

    private SearchResponse sendQuery(QueryBuilder queryBuilder, Integer limit) {
        return client.prepareSearch(PhotonIndex.NAME).
                setSearchType(SearchType.QUERY_THEN_FETCH).
                setQuery(queryBuilder).
                setSize(limit).
                setExplain(true).
                setTimeout(queryTimeout).
                execute().
                actionGet();

    }
}
