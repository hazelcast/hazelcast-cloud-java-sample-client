package com.hazelcast.cloud;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cloud.model.CityJsonSerializer;
import com.hazelcast.cloud.model.CountryJsonSerializer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;

import static com.hazelcast.client.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.client.properties.ClientProperty.STATISTICS_ENABLED;
import static com.hazelcast.cloud.model.City.newCity;
import static com.hazelcast.cloud.model.Country.newCountry;

public class SqlJsonClient {

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.setProperty(STATISTICS_ENABLED.getName(), "true");
        config.setProperty(HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "YOUR_CLUSTER_DISCOVERY_TOKEN");
        config.setProperty("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL");
        config.setClusterName("YOUR_CLUSTER_NAME");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        createMappingForCountries(client);

        populateCountriesWithMap(client);
        selectAllCountries(client);

        populateCities(client);
        createMappingForCities(client);

        selectCitiesByCountry(client, "AU");

        selectCountriesAndCities(client);

        System.out.println("done");
        System.exit(0);
    }

    private static void createMappingForCountries(HazelcastInstance client) {
        String mappingSql = ""
            + "CREATE OR REPLACE MAPPING country("
            + " __key VARCHAR,"
            + " isoCode VARCHAR,"
            + " country VARCHAR)"
            + " TYPE IMap"
            + " OPTIONS ("
            + "     'keyFormat' = 'java',"
            + "     'keyJavaClass' = 'java.lang.String',"
            + "     'valueFormat' = 'json-flat'"
            + " )";

        try (SqlResult rs = client.getSql().execute(mappingSql)) {
            rs.updateCount();
            System.out.println("Mapping for countries has been created");
        }
    }

    private static void populateCountriesWithMap(HazelcastInstance client) {
        System.out.println("Populate Countries with map");
        IMap<String, HazelcastJsonValue> countries = client.getMap("country");
        countries.put("AU", CountryJsonSerializer.countryAsJson(newCountry("AU", "Australia")));
        countries.put("EN", CountryJsonSerializer.countryAsJson(newCountry("EN", "England")));
        countries.put("US", CountryJsonSerializer.countryAsJson(newCountry("US", "United States")));
        countries.put("CZ", CountryJsonSerializer.countryAsJson(newCountry("US", "Czech Republic")));
    }

    private static void selectAllCountries(HazelcastInstance client) {
        String sql = "SELECT c.country from country c";
        System.out.println("sql = " + sql);
        try (SqlResult result = client.getSql().execute(sql)) {
            result.forEach(row -> System.out.println("country=" + row.getObject("country")));
        }
    }

    private static void populateCities(HazelcastInstance client) {
        System.out.println("Populate cities");
        IMap<Integer, HazelcastJsonValue> cities = client.getMap("city");
        cities.put(1, CityJsonSerializer.cityAsJson(newCity("AU", "Canberra", 354644)));
        cities.put(2, CityJsonSerializer.cityAsJson(newCity("CZ", "Prague", 1227332)));
        cities.put(3, CityJsonSerializer.cityAsJson(newCity("EN", "London", 8174100)));
        cities.put(4, CityJsonSerializer.cityAsJson(newCity("US", "Washington, DC", 601723)));
    }


    private static void createMappingForCities(HazelcastInstance client) {
        String mappingSql = ""
            + "CREATE OR REPLACE MAPPING city("
            + " __key INT ,"
            + " country VARCHAR ,"
            + " city VARCHAR,"
            + " population BIGINT)"
            + " TYPE IMap"
            + " OPTIONS ("
            + "     'keyFormat' = 'java',"
            + "     'keyJavaClass' = 'java.lang.Integer',"
            + "     'valueFormat' = 'compact',"
            + "     'valueCompactTypeName' = 'city'"
            + " )";

        try (SqlResult rs = client.getSql().execute(mappingSql)) {
            rs.updateCount();
            System.out.println("Mapping for cities has been created");
        }
    }

    private static void selectCitiesByCountry(HazelcastInstance client, String country) {
        String sql = "SELECT city, population FROM city where country=?";
        System.out.println("sql = " + sql);
        try (SqlResult result = client.getSql().execute(sql, country)) {
            result.forEach(row ->
                System.out.printf("city=%s, population=%s%n", row.getObject("city"), row.getObject("population"))
            );
        }
    }

    private static void selectCountriesAndCities(HazelcastInstance client) {

        String sql = ""
            + "SELECT c.isoCode, c.country, t.city, t.population"
            + "  FROM country c"
            + "       JOIN city t ON c.isoCode = t.country";

        System.out.printf("%4s | %15s | %20s | %15s |%n", "iso", "country", "city", "population");

        try (SqlResult result = client.getSql().execute(sql)) {
            result.forEach(row -> {
                System.out.printf("%4s | %15s | %20s | %15s |%n",
                    row.getObject("isoCode"),
                    row.getObject("country"),
                    row.getObject("city"),
                    row.getObject("population")
                );
            });
        }
    }

}
