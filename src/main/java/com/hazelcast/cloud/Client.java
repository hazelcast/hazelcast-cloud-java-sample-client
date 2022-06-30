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

import static com.hazelcast.cloud.model.City.newCity;
import static com.hazelcast.cloud.model.Country.newCountry;

/*
 * This is a boilerplate client application that connects to your Hazelcast Viridian cluster.
 * See: https://docs.hazelcast.com/cloud/get-started
 * 
 * Snippets of this code are included as examples in our documentation,
 * using the tag:: comments.
 */
public class Client {

    public static void main(String[] args) {
        // Configure the client to connect to the cluster.
        // tag::config[]
        ClientConfig config = new ClientConfig();
        /* Allow the client to resend requests to the cluster.
         * If the client does not receive a response from the cluster for any reason such as connectivity,
         * the client will resend the request.
         * See https://docs.hazelcast.org/docs/latest/javadoc/com/hazelcast/client/config/ClientNetworkConfig.html#setRedoOperation-boolean-
        */
        config.getNetworkConfig().setRedoOperation(true);
        /* The cluster discovery token is a unique token that maps to the current IP address of the cluster.
         * Cluster IP addresses may change.
         * This token allows clients to find out the current IP address
         * of the cluster and connect to it.
        */
        config.getNetworkConfig().getCloudConfig()
            .setDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN")
            .setEnabled(true);
        // tag::env[]
	    // Define which environment to use such as production, uat, or dev
        config.setProperty("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL");
        // end::env[]
        config.setClusterName("YOUR_CLUSTER_NAME");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        // end::config[]

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
        final String mappingQuery = ""
            + "CREATE OR REPLACE MAPPING cities TYPE IMap"
            + " OPTIONS ("
            + "     'keyFormat' = 'java',"
            + "     'keyJavaClass' = 'java.lang.String',"
            + "     'valueFormat' = 'java',"
            + "     'valueJavaClass' = 'java.lang.String'"
            + " )";
        try (SqlResult ignored = client.getSql().execute(mappingQuery)) {
            System.out.println("The mapping has been created successfully.");
        }
        System.out.println("--------------------");

        System.out.println("Deleting data via SQL...");
        try (SqlResult ignored = client.getSql().execute("DELETE FROM cities")) {
            System.out.println("The data has been deleted successfully.");
        }
        System.out.println("--------------------");
        
        System.out.println("Inserting data via SQL...");
        String insertQuery = ""
            + "INSERT INTO cities VALUES"
            + "('Australia','Canberra'),"
            + "('Croatia','Zagreb'),"
            + "('Czech Republic','Prague'),"
            + "('England','London'),"
            + "('Turkey','Ankara'),"
            + "('United States','Washington, DC');";
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

        createMappingForCities(client);

        populateCities(client);

        selectCitiesByCountry(client, "AU");

        selectCountriesAndCities(client);

        System.exit(0);
    }

    private static void createMappingForCountries(HazelcastInstance client) {
        //see: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps#json-objects
        System.out.println("Creating mapping for countries...");

        String mappingSql = ""
            + "CREATE OR REPLACE MAPPING country("
            + "     __key VARCHAR,"
            + "     isoCode VARCHAR,"
            + "     country VARCHAR"
            + ") TYPE IMap"
            + " OPTIONS ("
            + "     'keyFormat' = 'java',"
            + "     'keyJavaClass' = 'java.lang.String',"
            + "     'valueFormat' = 'json-flat'"
            + " )";

        try (SqlResult rs = client.getSql().execute(mappingSql)) {
            rs.updateCount();
            System.out.println("Mapping for countries has been created");
        }
        System.out.println("--------------------");
    }

    private static void populateCountriesWithMap(HazelcastInstance client) {
        // see: https://docs.hazelcast.com/hazelcast/5.0/data-structures/creating-a-map#writing-json-to-a-map
        System.out.println("Populating 'countries' map with JSON values...");

        IMap<String, HazelcastJsonValue> countries = client.getMap("country");
        countries.put("AU", CountryJsonSerializer.countryAsJson(newCountry("AU", "Australia")));
        countries.put("EN", CountryJsonSerializer.countryAsJson(newCountry("EN", "England")));
        countries.put("US", CountryJsonSerializer.countryAsJson(newCountry("US", "United States")));
        countries.put("CZ", CountryJsonSerializer.countryAsJson(newCountry("CZ", "Czech Republic")));

        System.out.println("The 'countries' map has been populated.");
        System.out.println("--------------------");
    }

    private static void selectAllCountries(HazelcastInstance client) {
        String sql = "SELECT c.country from country c";
        System.out.println("Select all countries with sql = " + sql);
        try (SqlResult result = client.getSql().execute(sql)) {
            result.forEach(row -> System.out.println("country = " + row.getObject("country")));
        }
        System.out.println("--------------------");
    }

    private static void createMappingForCities(HazelcastInstance client) {
        //see: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps#json-objects
        System.out.println("Creating mapping for cities...");

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
        System.out.println("--------------------");
    }

    private static void populateCities(HazelcastInstance client) {
        // see: https://docs.hazelcast.com/hazelcast/5.0/data-structures/creating-a-map#writing-json-to-a-map
        System.out.println("Populating 'city' map with JSON values...");

        IMap<Integer, HazelcastJsonValue> cities = client.getMap("city");
        cities.put(1, CityJsonSerializer.cityAsJson(newCity("AU", "Canberra", 354644)));
        cities.put(2, CityJsonSerializer.cityAsJson(newCity("CZ", "Prague", 1227332)));
        cities.put(3, CityJsonSerializer.cityAsJson(newCity("EN", "London", 8174100)));
        cities.put(4, CityJsonSerializer.cityAsJson(newCity("US", "Washington, DC", 601723)));

        System.out.println("The 'city' map has been populated.");
        System.out.println("--------------------");
    }

    private static void selectCitiesByCountry(HazelcastInstance client, String country) {
        String sql = "SELECT city, population FROM city where country=?";
        System.out.println("--------------------");
        System.out.println("Select city and population with sql = " + sql);
        try (SqlResult result = client.getSql().execute(sql, country)) {
            result.forEach(row ->
                System.out.printf("city = %s, population = %s%n", row.getObject("city"), row.getObject("population"))
            );
        }
        System.out.println("--------------------");
    }

    private static void selectCountriesAndCities(HazelcastInstance client) {
        String sql = ""
            + "SELECT c.isoCode, c.country, t.city, t.population"
            + "  FROM country c"
            + "       JOIN city t ON c.isoCode = t.country";

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
        System.out.println("--------------------");
    }

}
