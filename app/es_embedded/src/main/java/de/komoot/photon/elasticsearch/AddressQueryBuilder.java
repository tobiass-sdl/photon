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
    private static final float StateBoost = 0.1f; // state is unreliable - some locations have e.g. "NY", some "New York".
    private static final float CountyBoost = 4.0f;
    private static final float CityBoost = 3.0f;
    private static final float PostalCodeBoost = 7.0f;
    private static final float DistrictBoost = 2.0f;
    private static final float StreetBoost = 5.0f; // we filter streets in the wrong city / district / ... so we can use a high boost value
    private static final float HouseNumberBoost = 10.0f;
    private static final float HouseNumberUnmatchedBoost = 5f;
    private static final float FactorForWrongLanguage = 0.1f;

    private final String[] languages;
    private final String language;

    private BoolQueryBuilder query = QueryBuilders.boolQuery();

    private BoolQueryBuilder cityFilter;

    private boolean lenient;

    public AddressQueryBuilder(boolean lenient, String language, String[] languages) {
        this.lenient = lenient;
        this.language = language;
        this.languages = languages;
    }

    public QueryBuilder getQuery() {
        if(lenient) {
            query.minimumShouldMatch("10%");
        }

        return query;
    }

    public AddressQueryBuilder addCountryCode(String countryCode) {
        if(countryCode == null) return this;

        query.filter(QueryBuilders.termQuery(Constants.COUNTRYCODE, countryCode.toUpperCase()));
        return this;
    }

    public AddressQueryBuilder addState(String state, boolean hasMoreDetails) {
        if(state == null) return this;

        var stateQuery = GetNameOrFieldQuery(Constants.STATE, state, StateBoost, "state", hasMoreDetails);
        query.should(stateQuery);
        return this;
    }

    public AddressQueryBuilder addCounty(String county, boolean hasMoreDetails) {
        if(county == null) return this;

        AddNameOrFieldQuery(Constants.COUNTY, county, CountyBoost, "county", hasMoreDetails);
        return this;
    }

    public AddressQueryBuilder addCity(String city, boolean hasDistrict, boolean hasStreet) {
        if(city == null) return this;

        boolean shouldMatchCityEntry = !hasStreet || lenient;

        if(hasDistrict && !shouldMatchCityEntry) {
            AddFuzzyQuery(Constants.CITY, city, CityBoost);
        }
        else {
            BoolQueryBuilder combinedQuery;

            var cityQuery = GetFuzzyQuery(Constants.CITY, city).boost(CityBoost);

            var cityNameBoost = hasStreet ? 0.75f * CityBoost : (hasDistrict ? CityBoost : 1.25f * CityBoost);
            var cityNameQuery = GetFuzzyQuery(Constants.NAME, city)
                    .filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE,"city"))
                    .boost(cityNameBoost);

            if(hasDistrict) {
                combinedQuery = QueryBuilders.boolQuery()
                        .should(cityQuery)
                        .should(cityNameQuery);
            }
            else {
                var notCityFilter = QueryBuilders.boolQuery().mustNot(cityQuery);
                var districtQuery = GetFuzzyQuery(Constants.DISTRICT, city)
                        .boost(0.95f * CityBoost)
                        .filter(notCityFilter);


                if(shouldMatchCityEntry) {
                    var districtNameQuery = GetFuzzyQuery(Constants.NAME, city)
                            .filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, "district"))
                            .boost(0.99f * CityBoost)
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

            addToCityFilter(combinedQuery);
            AddQuery(combinedQuery);
        }

        return this;
    }

    private void addToCityFilter(QueryBuilder query) {
        if(cityFilter == null)
        {
            cityFilter = QueryBuilders.boolQuery();
        }

        cityFilter.should(query);
    }

    public AddressQueryBuilder addPostalCode(String postalCode) {
        if(postalCode == null) return this;

        Fuzziness fuzziness = lenient ? Fuzziness.AUTO : Fuzziness.ZERO;

        QueryBuilder query;
        if(StringUtils.containsWhitespace(postalCode)) {
            query = QueryBuilders.matchQuery(Constants.POSTCODE, postalCode)
                    .fuzziness(fuzziness.asString())
                    .boost(PostalCodeBoost);
        }
        else {
            query = QueryBuilders.fuzzyQuery(Constants.POSTCODE, postalCode)
                    .fuzziness(fuzziness)
                    .boost(PostalCodeBoost);
        }

        addToCityFilter(query);
        AddQuery(query);

        return this;
    }

    public AddressQueryBuilder addDistrict(String district, boolean hasMoreDetails) {
        if(district == null) return this;

        AddNameOrFieldQuery(Constants.DISTRICT, district, DistrictBoost, "district", hasMoreDetails);
        return this;
    }

    public AddressQueryBuilder addStreetAndHouseNumber(String street, String houseNumber) {
        if(street == null) return this;

        var fieldQuery = GetFuzzyQuery(Constants.STREET, street);
        var isStreetQuery = QueryBuilders.termQuery(Constants.OBJECT_TYPE, "street");

        BoolQueryBuilder streetQueryBuilder;

        if(houseNumber == null || lenient) {
            var nameFieldQuery = GetFuzzyQuery(Constants.NAME, street).filter(isStreetQuery);
            streetQueryBuilder = QueryBuilders.boolQuery()
                                        .should(fieldQuery)
                                        .should(nameFieldQuery);
        }
        else {
            streetQueryBuilder = fieldQuery;
        }

        streetQueryBuilder.boost(StreetBoost);

        if(houseNumber != null)
        {
            BoolQueryBuilder houseNumberQuery;
            if(lenient) {
                // either match the street or no street
                query.filter(QueryBuilders.boolQuery()
                        .should(streetQueryBuilder)
                        .should(QueryBuilders.boolQuery()
                                .mustNot(QueryBuilders.existsQuery(Constants.STREET + ".default"))
                                .mustNot(isStreetQuery)));

                var hasHouseNumber = QueryBuilders.existsQuery(Constants.HOUSENUMBER);

                query.should(QueryBuilders.boolQuery()
                        .must(streetQueryBuilder)
                        .mustNot(hasHouseNumber)
                        .boost(0.5f));

                houseNumberQuery = QueryBuilders.boolQuery()
                        .should(QueryBuilders.boolQuery()
                                .mustNot(hasHouseNumber)
                                .boost(HouseNumberUnmatchedBoost))
                        .should(QueryBuilders.matchPhraseQuery(Constants.HOUSENUMBER, houseNumber)
                                .boost(HouseNumberBoost));
            }
            else {
                houseNumberQuery = QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchPhraseQuery(Constants.HOUSENUMBER, houseNumber))
                        .boost(HouseNumberBoost);
            }

            houseNumberQuery.filter(streetQueryBuilder);
            if(cityFilter != null) {
                houseNumberQuery.filter(cityFilter);
            }

            AddQuery(houseNumberQuery);
        }
        else {
            query.should(streetQueryBuilder);
        }

        return this;
    }

    private BoolQueryBuilder GetFuzzyQuery(String name, String value)
    {
        var or = QueryBuilders.boolQuery();

        or.should(QueryBuilders.matchPhraseQuery(name + ".default", value)
                .boost(FactorForWrongLanguage));
        for(String lang : languages)
        {
            float boost = lang.equals(language) ? 1.0f : FactorForWrongLanguage;
            or.should(QueryBuilders.matchPhraseQuery(name + '.' + lang, value)
                    .boost(boost));
        }

        or.minimumShouldMatch("1");
        return or;
    }

    private void AddFuzzyQuery(String name, String value, float boost) {
        var fuzzyQuery = GetFuzzyQuery(name, value).boost(boost);
        if(isCityRelatedField(name)){
            addToCityFilter(fuzzyQuery);
        }

        AddQuery(fuzzyQuery);
    }

    private static boolean isCityRelatedField(String name) {
        return Objects.equals(name, Constants.POSTCODE) || Objects.equals(name, Constants.CITY) || Objects.equals(name, Constants.DISTRICT);
    }

    private void AddNameOrFieldQuery(String field, String value, float boost, String objectType, boolean hasMoreDetails) {
        var query = GetNameOrFieldQuery(field, value, boost, objectType, hasMoreDetails);
        if(isCityRelatedField(field)) {
            addToCityFilter(query);
        }

        AddQuery(query);
    }

    private BoolQueryBuilder GetNameOrFieldQuery(String field, String value, float boost, String objectType, boolean hasMoreDetails) {
        if(hasMoreDetails && !lenient)
        {
            return GetFuzzyQuery(field, value).boost(boost);
        }
        else {
            var fieldQuery = GetFuzzyQuery(field, value);
            var nameFieldQuery = GetFuzzyQuery(Constants.NAME, value);
            return QueryBuilders.boolQuery()
                    .should(fieldQuery)
                    .should(nameFieldQuery.filter(QueryBuilders.termQuery(Constants.OBJECT_TYPE, objectType)))
                    .boost(boost);
        }
    }

    private void AddQuery(QueryBuilder clause) {
        if(lenient) {
            query.should(clause);
        }
        else {
            query.must(clause);
        }
    }
}
