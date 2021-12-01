package com.hazelcast.cloud;

import java.util.Random;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cloud.model.CityJsonSerializer;
import com.hazelcast.cloud.model.CountryJsonSerializer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import static com.hazelcast.client.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.client.properties.ClientProperty.STATISTICS_ENABLED;
import static com.hazelcast.cloud.model.City.newCity;
import static com.hazelcast.cloud.model.Country.newCountry;

/**
 * This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
 * <p>
 * See: <a href="https://docs.cloud.hazelcast.com/docs/java-client">https://docs.cloud.hazelcast.com/docs/java-client</a>
 */
public class Client {

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.setProperty(STATISTICS_ENABLED.getName(), "true");
        config.setProperty(HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "YOUR_CLUSTER_DISCOVERY_TOKEN");
        config.setProperty("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL");
        config.setClusterName("YOUR_CLUSTER_NAME");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        System.out.println("Connection Successful!");

        // the 'mapExample' is an example with an infinite loop inside, so if you'd like to try other examples,
        // don't forget to comment out the following line
        mapExample(client);

        //sqlExample(client);

        //jsonSerializationExample(client);
    }

    /**
     * This example shows how to work with Hazelcast maps.
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void mapExample(HazelcastInstance client) {
        System.out.println("Now the map named 'map' will be filled with random entries.");

        IMap<String, String> map = client.getMap("map");
        Random random = new Random();
        int iterationCounter = 0;
        while (true) {
            int randomKey = random.nextInt(100_000);
            map.put("key-" + randomKey, "value-" + randomKey);
            map.get("key-" + random.nextInt(100_000));
            if (++iterationCounter == 10) {
                iterationCounter = 0;
                System.out.println("Current map size: " + map.size());
            }
        }
    }

    /**
     * This example shows how to work with Hazelcast SQL queries.
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void sqlExample(HazelcastInstance client) {

        System.out.println("Creating a mapping...");
        // See: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps
        try (SqlResult ignored = client.getSql().execute(
            "CREATE MAPPING cities TYPE IMap OPTIONS (" +
                "'keyFormat' = 'java'," +
                "'keyJavaClass' = 'java.lang.String'," +
                "'valueFormat' = 'java'," +
                "'valueJavaClass' = 'java.lang.String')")) {
            System.out.println("The mapping has been created successfully.");
        }

        System.out.println("--------------------");
        System.out.println("Inserting data via SQL...");

        String insertQuery = "INSERT INTO cities VALUES" +
            "('Australia','Canberra')," +
            "('Croatia','Zagreb')," +
            "('Czech Republic','Prague')," +
            "('England','London')," +
            "('Turkey','Ankara')," +
            "('United States','Washington, DC');";
        try (SqlResult ignored = client.getSql().execute(insertQuery)) {
            System.out.println("The data has been inserted successfully.");
        }

        System.out.println("--------------------");
        System.out.println("Retrieving all the data via SQL...");
        try (SqlResult result = client.getSql().execute("SELECT * FROM cities")) {

            for (SqlRow row : result) {
                String country = row.getObject(0);
                String city = row.getObject(1);
                System.out.printf("%s - %s\n", country, city);
            }
        }

        System.out.println("--------------------");
        System.out.println("Retrieving a city name via SQL...");
        try (SqlResult result = client.getSql()
            .execute("SELECT __key, this FROM cities WHERE __key = ?", "United States")) {

            for (SqlRow row : result) {
                String country = row.getObject("__key");
                String city = row.getObject("this");
                System.out.printf("Country name: %s; City name: %s\n", country, city);
            }
        }
        System.out.println("--------------------");
        System.exit(0);
    }

    /**
     * This example shows how to work with Hazelcast SQL queries via Maps that contains
     * JSON serialized values.
     * - Select single json element data from a Map
     * - Select data from Map with filtering
     * - Join data from 2 Maps and select json elements
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void jsonSerializationExample(HazelcastInstance client) {
        createMappingForCountries(client);

        populateCountriesWithMap(client);
        selectAllCountries(client);

        populateCities(client);
        createMappingForCities(client);

        selectCitiesByCountry(client, "AU");

        selectCountriesAndCities(client);

        System.out.println("--------------------");
        System.exit(0);
    }

    private static void createMappingForCountries(HazelcastInstance client) {
        //see: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps#json-objects
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
        // see: https://docs.hazelcast.com/hazelcast/5.0/data-structures/creating-a-map#writing-json-to-a-map
        System.out.println("Populate Countries with map - values as JSON");
        IMap<String, HazelcastJsonValue> countries = client.getMap("country");
        countries.put("AU", CountryJsonSerializer.countryAsJson(newCountry("AU", "Australia")));
        countries.put("EN", CountryJsonSerializer.countryAsJson(newCountry("EN", "England")));
        countries.put("US", CountryJsonSerializer.countryAsJson(newCountry("US", "United States")));
        countries.put("CZ", CountryJsonSerializer.countryAsJson(newCountry("US", "Czech Republic")));
    }

    private static void selectAllCountries(HazelcastInstance client) {
        String sql = "SELECT c.country from country c";
        System.out.println("--------------------");
        System.out.println("Select all countries with sql = " + sql);
        try (SqlResult result = client.getSql().execute(sql)) {
            result.forEach(row -> System.out.println("country=" + row.getObject("country")));
        }
    }

    private static void populateCities(HazelcastInstance client) {
        // see: https://docs.hazelcast.com/hazelcast/5.0/data-structures/creating-a-map#writing-json-to-a-map
        System.out.println("--------------------");
        System.out.println("Populate cities");
        IMap<Integer, HazelcastJsonValue> cities = client.getMap("city");
        cities.put(1, CityJsonSerializer.cityAsJson(newCity("AU", "Canberra", 354644)));
        cities.put(2, CityJsonSerializer.cityAsJson(newCity("CZ", "Prague", 1227332)));
        cities.put(3, CityJsonSerializer.cityAsJson(newCity("EN", "London", 8174100)));
        cities.put(4, CityJsonSerializer.cityAsJson(newCity("US", "Washington, DC", 601723)));
    }


    private static void createMappingForCities(HazelcastInstance client) {
        //see: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps#json-objects
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
            + "     'valueFormat' = 'json-flat'"
            + " )";

        try (SqlResult rs = client.getSql().execute(mappingSql)) {
            rs.updateCount();
            System.out.println("Mapping for cities has been created");
        }
    }

    private static void selectCitiesByCountry(HazelcastInstance client, String country) {
        String sql = "SELECT city, population FROM city where country=?";
        System.out.println("--------------------");
        System.out.println("Select city and population with sql = " + sql);
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

        System.out.println("--------------------");
        System.out.println("Select country and city data in query that joins tables");
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
