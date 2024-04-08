package de.komoot.photon;

import de.komoot.photon.query.PhotonRequest;

public class StructuredPhotonRequest {
    private final String language;
    private int limit = 15;

    private String countryCode;
    private String state;
    private String county;
    private String city;
    private String postCode;
    private String district;
    private String street;
    private String houseNumber;

    public StructuredPhotonRequest(String language) {
        this.language = language;
    }

    public boolean getDebug() {
        return false;
    }

    public String getLanguage() {
        return language;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        if (limit != null) {
            this.limit = Integer.max(Integer.min(limit, 50), 1);
        }
    }


    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getPostCode() {
        return postCode;
    }

    public void setPostCode(String postCode) {
        this.postCode = postCode;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getHouseNumber() {
        return houseNumber;
    }

    public void setHouseNumber(String houseNumber) {
        this.houseNumber = houseNumber;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
