package com.hazelcast.cloud.model;

public class City {

    private String country;
    private String city;
    private int population;

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getPopulation() {
        return population;
    }

    public void setPopulation(int population) {
        this.population = population;
    }

    public static City newCity(String country, String cityName, int population) {
        City city = new City();
        city.country = country;
        city.city = cityName;
        city.population = population;
        return city;
    }

}
