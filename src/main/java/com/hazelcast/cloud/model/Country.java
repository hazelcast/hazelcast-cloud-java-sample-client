package com.hazelcast.cloud.model;

public class Country {

    private String isoCode;
    private String country;

    public String getIsoCode() {
        return isoCode;
    }

    public void setIsoCode(String isoCode) {
        this.isoCode = isoCode;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public static Country newCountry(String isoCode, String countryName) {
        Country country = new Country();
        country.isoCode = isoCode;
        country.country = countryName;
        return country;
    }
}
