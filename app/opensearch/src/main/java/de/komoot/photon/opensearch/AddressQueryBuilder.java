package de.komoot.photon.opensearch;

import de.komoot.photon.Constants;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.QueryBuilders;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.BoolQueryBuilder;

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

    private BoolQuery.Builder query = QueryBuilders.bool();

    private BoolQuery.Builder cityFilter;

    private boolean lenient;

    private State state = State.City;

    public AddressQueryBuilder(boolean lenient, String language, String[] languages)
    {
        this.lenient = lenient;
        this.language = language;
        this.languages = languages;
    }

    public Query getQuery()
    {
        if(lenient)
        {
            var intermediateQuery = query.build();
            var clauses = intermediateQuery.should();
            var minShouldMatch = Math.max(intermediateQuery.must().isEmpty() ? 1 : 0, Math.min(3 * clauses.size() / 5, clauses.size() - 1));
            query.minimumShouldMatch(Integer.toString(minShouldMatch));

            return query.build().toQuery();
        }
        else {
            return query.build().toQuery();
        }
    }

    public AddressQueryBuilder addCountryCode(String countryCode)
    {
        if(countryCode == null) return this;

        query.must(QueryBuilders.term().field(Constants.COUNTRYCODE).value(FieldValue.of(countryCode.toUpperCase())).build().toQuery());
        return this;
    }

    public AddressQueryBuilder addState(String state, boolean hasMoreDetails)
    {
        if(state == null) return this;

        var stateQuery = GetNameOrFieldQuery(Constants.STATE, state, StateBoost, "state", hasMoreDetails)
                .build()
                .toQuery();
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
            BoolQuery.Builder combinedQueryBuilder;

            var cityQuery = GetFuzzyQuery(Constants.CITY, city).boost(CityBoost).build().toQuery();

            var cityNameQuery = GetFuzzyQuery(Constants.NAME, city)
                    .filter(QueryBuilders.term().field(Constants.OBJECT_TYPE).value(FieldValue.of("city")).build().toQuery())
                    .boost(CityBoost)
                    .build()
                    .toQuery();

            if(hasDistrict) {
                combinedQueryBuilder = QueryBuilders.bool()
                        .should(cityQuery)
                        .should(cityNameQuery);
            }
            else {
                var notCityFilter = QueryBuilders.bool().mustNot(cityQuery).build().toQuery();
                var districtQuery = GetFuzzyQuery(Constants.DISTRICT, city)
                        .boost(0.95f * CityBoost)
                        .filter(notCityFilter)
                        .build()
                        .toQuery();


                if(shouldMatchCityEntry) {
                    var districtNameQuery = GetFuzzyQuery(Constants.NAME, city)
                            .filter(QueryBuilders.term().field(Constants.OBJECT_TYPE).value(FieldValue.of("district"))
                                    .boost(0.99f * CityBoost)
                                    .build()
                                    .toQuery())
                            .filter(notCityFilter)
                            .build()
                            .toQuery();
                    combinedQueryBuilder = QueryBuilders.bool()
                            .should(cityQuery)
                            .should(cityNameQuery)
                            .should(districtQuery)
                            .should(districtNameQuery);
                }
                else {
                    combinedQueryBuilder = QueryBuilders.bool().should(cityQuery).should(districtQuery);
                }
            }

            var combinedQuery = combinedQueryBuilder.build().toQuery();
            addToCityFilter(combinedQuery);
            AddQuery(combinedQuery);
        }

        return this;
    }

    private void addToCityFilter(Query query)
    {
        if(cityFilter == null)
        {
            cityFilter = QueryBuilders.bool();
        }

        cityFilter.should(query);
    }

    public AddressQueryBuilder addPostalCode(String postalCode)
    {
        VerifyCityState();

        if(postalCode == null) return this;

        Fuzziness fuzziness = lenient ? Fuzziness.AUTO : Fuzziness.ZERO;

        Query query;
        if(StringUtils.containsWhitespace(postalCode)) {
            query = QueryBuilders.match()
                    .field(Constants.POSTCODE)
                    .query(FieldValue.of(postalCode))
                    .fuzziness(fuzziness.asString())
                    .boost(PostalCodeBoost)
                    .build()
                    .toQuery();
        }
        else {
            query = QueryBuilders.fuzzy()
                    .field(Constants.POSTCODE)
                    .value(FieldValue.of(postalCode))
                    .fuzziness(fuzziness.asString())
                    .boost(PostalCodeBoost)
                    .build()
                    .toQuery();
        }

        addToCityFilter(query);
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
        BoolQuery.Builder streetQueryBuilder = null;
        if(houseNumber != null && false)
        {
            streetQueryBuilder = GetFuzzyQuery(Constants.STREET, street).boost(StreetBoost);
        }
        else {
            var fieldQuery = GetFuzzyQuery(Constants.STREET, street);
            var nameFieldQuery = GetFuzzyQuery(Constants.NAME, street);
            streetQueryBuilder = QueryBuilders.bool().should(fieldQuery.build().toQuery())
                    .should(nameFieldQuery.filter(QueryBuilders.term().field(Constants.OBJECT_TYPE).value(FieldValue.of("street")).build().toQuery())
                            .build().toQuery())
                    .boost(StreetBoost);
        }

        if(lenient) {
            streetQueryBuilder = QueryBuilders.bool()
                    .should(streetQueryBuilder.build().toQuery())
                    .should(QueryBuilders.bool()
                            .mustNot(QueryBuilders.exists()
                                    .field(Constants.STREET)
                                    .boost(StreetBoost * 0.5f)
                                    .build().toQuery())
                            .build()
                            .toQuery());
        }

        if(lenient && cityFilter != null)
        {
            streetQueryBuilder.filter(cityFilter.build().toQuery());
        }

        var streetQuery = streetQueryBuilder.build().toQuery();
        query.should(streetQuery);

        if(houseNumber != null)
        {
            if(lenient) {
                query.must(QueryBuilders.bool()
                        .should(QueryBuilders.bool()
                                .mustNot(QueryBuilders.exists()
                                        .field(Constants.HOUSENUMBER)
                                        .build()
                                        .toQuery())
                                .build()
                                .toQuery())
                        .should(QueryBuilders.matchPhrase()
                                .field(Constants.HOUSENUMBER)
                                .query(houseNumber)
                                .build()
                                .toQuery())
                        .filter(streetQuery)
                        .boost(HouseNumberBoost)
                        .build()
                        .toQuery());
            }
            else {
                AddQuery(QueryBuilders.bool()
                        .must(QueryBuilders.matchPhrase()
                                .field(Constants.HOUSENUMBER)
                                .query(houseNumber)
                                .build()
                                .toQuery())
                        .filter(streetQuery)
                        .boost(HouseNumberBoost)
                        .build()
                        .toQuery());
            }
        }

        return this;
    }

    private void VerifyCityState()
    {
        if(state == State.Street) throw new UnsupportedOperationException("can't add city related values after street was set");
    }

    private BoolQuery.Builder GetFuzzyQuery(String name, String value)
    {
        var or = QueryBuilders.bool();

        or.should(QueryBuilders.matchPhrase().field(name + ".default").query(value).boost(FactorForWrongLanguage).build().toQuery());
        for(String lang : languages)
        {
            float boost = lang.equals(language) ? 1.0f : FactorForWrongLanguage;
            or = or.should(QueryBuilders.matchPhrase().field(name + '.' + lang).query(value).boost(boost).build().toQuery());
        }

        return or;
    }

    private void AddFuzzyQuery(String name, String value, float boost)
    {
        var fuzzyQuery = GetFuzzyQuery(name, value).boost(boost);
        if(isCityRelatedField(name)){
            addToCityFilter(fuzzyQuery.build().toQuery());
        }

        AddQuery(fuzzyQuery.build().toQuery());
    }

    private static boolean isCityRelatedField(String name)
    {
        return Objects.equals(name, Constants.POSTCODE) || Objects.equals(name, Constants.CITY) || Objects.equals(name, Constants.DISTRICT);
    }

    private void AddNameOrFieldQuery(String field, String value, float boost, String objectType, boolean hasMoreDetails) {
        var query = GetNameOrFieldQuery(field, value, boost, objectType, hasMoreDetails).build().toQuery();
        if(isCityRelatedField(field))
        {
            addToCityFilter(query);
        }

        AddQuery(query);
    }

    private BoolQuery.Builder GetNameOrFieldQuery(String field, String value, float boost, String objectType, boolean hasMoreDetails)
    {
        if(hasMoreDetails && !lenient)
        {
            return GetFuzzyQuery(field, value).boost(boost);
        }
        else {
            var fieldQuery = GetFuzzyQuery(field, value);
            var nameFieldQuery = GetFuzzyQuery(Constants.NAME, value);
            return QueryBuilders.bool()
                    .should(fieldQuery.build().toQuery())
                    .should(nameFieldQuery.filter(QueryBuilders.term()
                                .field(Constants.OBJECT_TYPE)
                                .value(FieldValue.of(objectType))
                                .build()
                                .toQuery())
                            .build()
                            .toQuery())
                    .boost(boost);
        }
    }

    private void AddQuery(Query clause) {
        if(lenient)
        {
            query.should(clause);
        }
        else {
            query.must(clause);
        }
    }
}
