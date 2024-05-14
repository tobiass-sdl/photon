package de.komoot.photon.elasticsearch;

import de.komoot.photon.Constants;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.cluster.routing.DelayedAllocationService;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Objects;

public class AddressQueryBuilder {

    private enum State {
        City,
        Street
    }

    private static final float StateBoost = 0.1f; // state is unreliable - some locations have e.g. "NY", some "New York".
    private static final float CountyBoost = 4.0f;
    private static final float CityBoost = 3.0f;
    private static final float PostalCodeBoost = 7.0f;
    private static final float DistrictBoost = 2.0f;
    private static final float StreetBoost = 1.5f;
    private static final float HouseNumberBoost = 0.6f;

    private static final float FactorForWrongLanguage = 0.1f;

    private final String[] languages;
    private final String language;

    private BoolQueryBuilder query = QueryBuilders.boolQuery();

    private BoolQueryBuilder cityFilter = QueryBuilders.boolQuery();

    private boolean lenient;

    private State state = State.City;

    public AddressQueryBuilder(boolean lenient, String language, String[] languages)
    {
        this.lenient = lenient;
        this.language = language;
        this.languages = languages;
    }

    public QueryBuilder getQuery()
    {
        if(lenient)
        {
            List<QueryBuilder> clauses = query.should();
            query.minimumShouldMatch(Math.max(query.must().isEmpty() ? 1 : 0, Math.min(3 * clauses.size() / 5, clauses.size() - 1)));

            return query;
        }
        else {
            return query;
        }
    }

    public AddressQueryBuilder addCountryCode(String countryCode)
    {
        if(countryCode == null) return this;

        query.must(QueryBuilders.termQuery(Constants.COUNTRYCODE, countryCode.toUpperCase()));
        return this;
    }

    public AddressQueryBuilder addState(String state, boolean hasMoreDetails)
    {
        if(state == null) return this;

        QueryBuilder stateQuery = GetNameOrFieldQuery(Constants.STATE, state, StateBoost, "state", hasMoreDetails);
        query.should(stateQuery); // state information is unreliable - NY vs New York etc.
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
        VerifyCityState();

        if(city == null) return this;

        boolean shouldMatchCityEntry = !hasStreet || lenient;

        if(hasDistrict && !shouldMatchCityEntry) {
            AddFuzzyQuery(Constants.CITY, city, CityBoost);
        }
        else {
            BoolQueryBuilder combinedQuery;

            QueryBuilder cityQuery = GetFuzzyQuery(Constants.CITY, city).boost(CityBoost);

            BoolQueryBuilder cityNameQuery = GetFuzzyQuery(Constants.NAME, city).filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "city")).boost(CityBoost);

            if(hasDistrict) {
                combinedQuery = QueryBuilders.boolQuery()
                        .should(cityQuery)
                        .should(cityNameQuery);
            }
            else {
                QueryBuilder notCityFilter = QueryBuilders.boolQuery().mustNot(cityQuery);
                QueryBuilder districtQuery = GetFuzzyQuery(Constants.DISTRICT, city)
                        .boost(0.95f * CityBoost)
                        .filter(notCityFilter);


                if(shouldMatchCityEntry) {
                    BoolQueryBuilder districtNameQuery = GetFuzzyQuery(Constants.NAME, city).filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "district")
                                    .boost(0.99f * CityBoost))
                            .filter(notCityFilter);
                    combinedQuery = QueryBuilders.boolQuery()
                            .should(cityQuery)
                            .should(cityNameQuery)
                            .should(districtQuery)
                            .should(districtNameQuery);
                }
                else {
                    combinedQuery = QueryBuilders.boolQuery().should(cityQuery).should(districtQuery);
                }
            }

            cityFilter.should(combinedQuery);
            AddQuery(combinedQuery);
        }

        return this;
    }

    public AddressQueryBuilder addPostalCode(String postalCode)
    {
        VerifyCityState();

        if(postalCode == null) return this;

        Fuzziness fuzziness = lenient ? Fuzziness.AUTO : Fuzziness.ZERO;

        QueryBuilder query;
        if(StringUtils.containsWhitespace(postalCode)) {
            query = QueryBuilders.matchQuery(Constants.POSTCODE, postalCode).fuzziness(fuzziness).boost(PostalCodeBoost);
        }
        else {
            query = QueryBuilders.fuzzyQuery(Constants.POSTCODE, postalCode).fuzziness(fuzziness).boost(PostalCodeBoost);
        }

        cityFilter.should(query);
        AddQuery(query);

        return this;
    }

    public AddressQueryBuilder addDistrict(String district, boolean hasMoreDetails)
    {
        VerifyCityState();

        if(district == null) return this;

        AddNameOrFieldQuery(Constants.DISTRICT, district, DistrictBoost, "district", hasMoreDetails);
        return this;
    }

    public AddressQueryBuilder addStreetAndHouseNumber(String street, String houseNumber)
    {
        if(street == null) return this;

        state = State.Street;
        BoolQueryBuilder streetQuery = null;
        if(houseNumber != null && false)
        {
            streetQuery = GetFuzzyQuery(Constants.STREET, street).boost(StreetBoost);
        }
        else {
            BoolQueryBuilder fieldQuery = GetFuzzyQuery(Constants.STREET, street);
            BoolQueryBuilder nameFieldQuery = GetFuzzyQuery(Constants.NAME, street);
            streetQuery = QueryBuilders.boolQuery().should(fieldQuery)
                    .should(nameFieldQuery.filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "street")))
                    .boost(StreetBoost);
        }

        if(lenient) {
            streetQuery = QueryBuilders.boolQuery()
                    .should(streetQuery)
                    .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(Constants.STREET).boost(StreetBoost * 0.5f)));
        }

        if(lenient && cityFilter.hasClauses())
        {
            streetQuery = streetQuery.filter(cityFilter);
        }

        query.should(streetQuery);

        if(houseNumber != null)
        {
            if(lenient) {
                query.must(QueryBuilders.boolQuery().should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(Constants.HOUSENUMBER)))
                        .should(QueryBuilders.matchPhraseQuery(Constants.HOUSENUMBER, houseNumber))
                        .filter(streetQuery)
                        .boost(HouseNumberBoost));
            }
            else {
                AddQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchPhraseQuery(Constants.HOUSENUMBER, houseNumber))
                        .filter(streetQuery)
                        .boost(HouseNumberBoost));
            }
        }

        return this;
    }

    private void VerifyCityState()
    {
        if(state == State.Street) throw new UnsupportedOperationException("can't add city related values after street was set");
    }

    private BoolQueryBuilder GetFuzzyQuery(String name, String value)
    {
        BoolQueryBuilder or = QueryBuilders.boolQuery();

        or.should(QueryBuilders.matchPhraseQuery(name + ".default", value).boost(FactorForWrongLanguage));
        for(String lang : languages)
        {
            float boost = lang.equals(language) ? 1.0f : FactorForWrongLanguage;
            or = or.should(QueryBuilders.matchPhraseQuery(name + '.' + lang, value).boost(boost));
        }

        return or;
    }

    private void AddFuzzyQuery(String name, String value, float boost)
    {
        QueryBuilder fuzzyQuery = GetFuzzyQuery(name, value).boost(boost);
        if(isCityRelatedField(name)){
            cityFilter.should(fuzzyQuery);
        }

        AddQuery(fuzzyQuery);
    }

    private static boolean isCityRelatedField(String name)
    {
        return Objects.equals(name, Constants.POSTCODE) || Objects.equals(name, Constants.CITY) || Objects.equals(name, Constants.DISTRICT);
    }

    private void AddNameOrFieldQuery(String field, String value, float boost, String objectType, boolean hasMoreDetails) {
        QueryBuilder query = GetNameOrFieldQuery(field, value, boost, objectType, hasMoreDetails);
        if(isCityRelatedField(field))
        {
            cityFilter.should(query);
        }

        AddQuery(query);
    }

    private QueryBuilder GetNameOrFieldQuery(String field, String value, float boost, String objectType, boolean hasMoreDetails)
    {
        if(hasMoreDetails && !lenient)
        {
            return GetFuzzyQuery(field, value).boost(boost);
        }
        else {
            QueryBuilder fieldQuery = GetFuzzyQuery(field, value);
            BoolQueryBuilder nameFieldQuery = GetFuzzyQuery(Constants.NAME, value);
            return QueryBuilders.boolQuery().should(fieldQuery)
                    .should(nameFieldQuery.filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, objectType)))
                    .boost(boost);
        }
    }

    private void AddQuery(QueryBuilder clause) {
        if(lenient)
        {
            query.should(clause);
        }
        else {
            query.must(clause);
        }
    }
}
