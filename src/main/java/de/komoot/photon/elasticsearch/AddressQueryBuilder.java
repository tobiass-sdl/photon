package de.komoot.photon.elasticsearch;

import de.komoot.photon.Constants;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;

import java.util.List;

public class AddressQueryBuilder {

    private static final float StateBoost = 5.0f;
    private static final float CountyBoost = 4.0f;
    private static final float CityBoost = 3.0f;
    private static final float PostalCodeBoost = 3.0f;
    private static final float DistrictBoost = 2.0f;
    private static final float StreetBoost = 1.0f;
    private static final float HouseNumberBoost = 1.0f;

    private static final float FactorForWrongLanguage = 0.6f;

    private final String[] languages;
    private final String language;

    private BoolQueryBuilder query = QueryBuilders.boolQuery();
    private boolean lenient;

    public AddressQueryBuilder(boolean lenient, String language, String[] languages)
    {
        System.out.println("lenient=" + lenient);
        this.lenient = lenient;
        this.language = language;
        this.languages = languages;
    }

    private BoolQueryBuilder GetFuzzyQuery(String name, String value)
    {
        BoolQueryBuilder or = QueryBuilders.boolQuery();

        or.should(QueryBuilders.fuzzyQuery(name + ".default", value).boost(FactorForWrongLanguage));
        for(String lang : languages)
        {
            float boost = lang.equals(language) ? 1.0f : FactorForWrongLanguage;
            or = or.should(QueryBuilders.fuzzyQuery(name + '.' + lang, value).boost(boost));
        }

        return or;
    }

    private void AddFuzzyQuery(String name, String value, float boost)
    {
        QueryBuilder fuzzyQuery = GetFuzzyQuery(name, value).boost(boost);
        AddQuery(fuzzyQuery);
    }

    private void AddNameOrFieldQuery(String field, String value, float boost, String objectType, boolean hasMoreDetails)
    {
        if(hasMoreDetails)
        {
            AddFuzzyQuery(field, value, boost);
        }
        else {
            BoolQueryBuilder fieldQuery = GetFuzzyQuery(field, value);
            BoolQueryBuilder nameFieldQuery = GetFuzzyQuery(Constants.NAME, value);
            BoolQueryBuilder combinedQuery = QueryBuilders.boolQuery().should(fieldQuery)
                    .should(nameFieldQuery.filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, objectType)))
                    .boost(boost);

            AddQuery(combinedQuery);
        }
    }

    public QueryBuilder getQuery()
    {
        if(lenient)
        {
            List<QueryBuilder> clauses = query.should();
            FunctionScoreQueryBuilder.FilterFunctionBuilder[] scores = new FunctionScoreQueryBuilder.FilterFunctionBuilder[clauses.size()];
            for(int i = 0; i < clauses.size(); ++i)
            {
                QueryBuilder clause = clauses.get(i);
                scores[i] = new FunctionScoreQueryBuilder.FilterFunctionBuilder(clause, ScoreFunctionBuilders.weightFactorFunction(clause.boost()));
            }

            return QueryBuilders.functionScoreQuery(query, scores).scoreMode(FiltersFunctionScoreQuery.ScoreMode.SUM);
        }
        else {
            return query;
        }
    }

    public AddressQueryBuilder addCountryCode(String countryCode)
    {
        if(countryCode == null) return this;

        query.must(QueryBuilders.termQuery(Constants.COUNTRYCODE, countryCode));
        return this;
    }

    public AddressQueryBuilder addState(String state, boolean hasMoreDetails)
    {
        if(state == null) return this;

        AddNameOrFieldQuery(Constants.STATE, state, StateBoost, "state", hasMoreDetails);
        return this;
    }

    public AddressQueryBuilder addCounty(String county, boolean hasMoreDetails)
    {
        if(county == null) return this;

        AddNameOrFieldQuery(Constants.COUNTY, county, CountyBoost, "county", hasMoreDetails);
        return this;
    }

    public AddressQueryBuilder addCity(String city, boolean hasDistrict, boolean hasStreet)
    {
        if(city == null) return this;

        if(hasDistrict && hasStreet)
        {
            AddFuzzyQuery(Constants.CITY, city, CityBoost);
        }
        else if(hasStreet) // street but no district -> try to match the claimed city also as district
        {
            QueryBuilder cityQuery = GetFuzzyQuery(Constants.CITY, city).boost(CityBoost);
            QueryBuilder districtQuery = GetFuzzyQuery(Constants.DISTRICT, city).boost(CityBoost * 0.95f);
            QueryBuilder combined = QueryBuilders.boolQuery().should(cityQuery).should(districtQuery);
            AddQuery(combined);
        }
        else {
            BoolQueryBuilder cityFieldQuery = GetFuzzyQuery(Constants.CITY, city);
            BoolQueryBuilder combinedQuery = QueryBuilders.boolQuery().should(cityFieldQuery)
                    .should(GetFuzzyQuery(Constants.NAME, city).filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "city")))
                    .should(GetFuzzyQuery(Constants.NAME, city).filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "district").boost(0.99f)))
                    .boost(CityBoost);

            AddQuery(combinedQuery);
        }

        return this;
    }

    private void AddQuery(QueryBuilder clause){
        if(lenient)
        {
            query.should(clause);
        }
        else {
            query.must(clause);
        }
    }

    public AddressQueryBuilder addPostalCode(String postalCode)
    {
        if(postalCode == null) return this;

        AddQuery(QueryBuilders.fuzzyQuery(Constants.POSTCODE, postalCode).boost(PostalCodeBoost));

        return this;
    }

    public AddressQueryBuilder addDistrict(String district, boolean hasMoreDetails)
    {
        if(district == null) return this;

        AddNameOrFieldQuery(Constants.DISTRICT, district, DistrictBoost, "district", hasMoreDetails);
        return this;
    }

    public AddressQueryBuilder addStreet(String street, boolean hasMoreDetails)
    {
        if(street == null) return this;

        AddNameOrFieldQuery(Constants.STREET, street, StreetBoost, "street", hasMoreDetails);
        return this;
    }

    public AddressQueryBuilder addHouseNumber(String houseNumber)
    {
        if(houseNumber == null) return this;

        if(lenient) {
            query.must(QueryBuilders.boolQuery().should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(Constants.HOUSENUMBER)))
                    .should(QueryBuilders.fuzzyQuery(Constants.HOUSENUMBER, houseNumber)).boost(HouseNumberBoost));
        }
        else {
            AddQuery(QueryBuilders.fuzzyQuery(Constants.HOUSENUMBER, houseNumber).boost(HouseNumberBoost));
        }

        return this;
    }
}
