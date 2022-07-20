package com.hazelcast.cloud;

import java.util.Properties;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cloud.model.City;
import com.hazelcast.cloud.model.Country;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

/**
 * This is boilerplate application that configures client to connect Hazelcast
 * Cloud cluster.
 * <p>
 * See: <a href="https://docs.hazelcast.com/cloud/java-client">https://docs.hazelcast.com/cloud/java-client</a>
 */
public class ClientWithSsl {

    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = ClientWithSsl.class.getClassLoader();
        Properties props = new Properties();
        props.setProperty("javax.net.ssl.keyStore", classLoader.getResource("client.keystore").toURI().getPath());
        props.setProperty("javax.net.ssl.keyStorePassword", "YOUR_SSL_PASSWORD");
        props.setProperty("javax.net.ssl.trustStore",
                classLoader.getResource("client.truststore").toURI().getPath());
        props.setProperty("javax.net.ssl.trustStorePassword", "YOUR_SSL_PASSWORD");
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));
        config.getNetworkConfig().getCloudConfig()
                .setDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN")
                .setEnabled(true);
        config.setProperty("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL");
        config.setClusterName("YOUR_CLUSTER_NAME");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        System.out.println("Connection Successful!");

        mapExample(client);

        //sqlExample(client);

        //jsonSerializationExample(client);

        client.shutdown();
    }

    /**
     * This example shows how to work with Hazelcast maps.
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void mapExample(HazelcastInstance client) {
        IMap<String, HazelcastJsonValue> cities = client.getMap("cities");
        cities.put("1", City.asJson("United Kingdom", "London", 9_540_576));
        cities.put("2", City.asJson("United Kingdom", "Manchester", 2_770_434));
        cities.put("3", City.asJson("United States", "New York", 19_223_191));
        cities.put("4", City.asJson("United States", "Los Angeles", 3_985_520));
        cities.put("5", City.asJson("Turkey", "Ankara", 5_309_690));
        cities.put("6", City.asJson("Turkey", "Istanbul", 15_636_243));
        cities.put("7", City.asJson("Brazil", "Sao Paulo", 22_429_800));
        cities.put("8", City.asJson("Brazil", "Rio de Janeiro", 13_634_274));

        int mapSize = cities.size();
        System.out.printf("'cities' map now contains %d entries.\n", mapSize);

        System.out.println("--------------------");
    }

    /**
     * This example shows how to work with Hazelcast SQL queries.
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void sqlExample(HazelcastInstance client) {
        SqlService sqlService = client.getSql();

        createMappingForCapitals(sqlService);

        clearCapitals(sqlService);

        populateCapitals(sqlService);

        selectAllCapitals(sqlService);

        selectCapitalNames(sqlService);
    }

    private static void createMappingForCapitals(SqlService sqlService) {
        System.out.println("Creating a mapping...");
        // See: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps

        String mappingQuery = ""
                + "CREATE OR REPLACE MAPPING capitals TYPE IMap"
                + " OPTIONS ("
                + "     'keyFormat' = 'varchar',"
                + "     'valueFormat' = 'varchar'"
                + " )";
        try (SqlResult ignored = sqlService.execute(mappingQuery)) {
            System.out.println("The mapping has been created successfully.");
        }
        System.out.println("--------------------");
    }

    private static void clearCapitals(SqlService sqlService) {
        System.out.println("Deleting data via SQL...");
        try (SqlResult ignored = sqlService.execute("DELETE FROM capitals")) {
            System.out.println("The data has been deleted successfully.");
        }
        System.out.println("--------------------");
    }

    private static void populateCapitals(SqlService sqlService) {
        System.out.println("Inserting data via SQL...");
        String insertQuery = ""
                + "INSERT INTO capitals VALUES"
                + "('Australia','Canberra'),"
                + "('Croatia','Zagreb'),"
                + "('Czech Republic','Prague'),"
                + "('England','London'),"
                + "('Turkey','Ankara'),"
                + "('United States','Washington, DC');";
        try (SqlResult ignored = sqlService.execute(insertQuery)) {
            System.out.println("The data has been inserted successfully.");
        }
        System.out.println("--------------------");
    }

    private static void selectAllCapitals(SqlService sqlService) {
        System.out.println("Retrieving all the data via SQL...");
        try (SqlResult result = sqlService.execute("SELECT * FROM capitals")) {

            for (SqlRow row : result) {
                String country = row.getObject(0);
                String city = row.getObject(1);
                System.out.printf("%s - %s\n", country, city);
            }
        }
        System.out.println("--------------------");
    }

    private static void selectCapitalNames(SqlService sqlService) {
        System.out.println("Retrieving the capital name via SQL...");
        try (SqlResult result = sqlService
                .execute("SELECT __key, this FROM capitals WHERE __key = ?", "United States")) {

            for (SqlRow row : result) {
                String country = row.getObject("__key");
                String city = row.getObject("this");
                System.out.printf("Country name: %s; Capital name: %s\n", country, city);
            }
        }
        System.out.println("--------------------");
    }

    /**
     * This example shows how to work with Hazelcast SQL queries via Maps that
     * contains JSON serialized values.
     *
     * <ul>
     *     <li>Select single json element data from a Map</li>
     *     <li>Select data from Map with filtering</li>
     *     <li>Join data from two Maps and select json elements</li>
     * </ul>
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void jsonSerializationExample(HazelcastInstance client) {
        SqlService sqlService = client.getSql();

        createMappingForCountries(sqlService);

        populateCountriesWithMap(client);

        selectAllCountries(sqlService);

        createMappingForCities(sqlService);

        populateCities(client);

        selectCitiesByCountry(sqlService, "AU");

        selectCountriesAndCities(sqlService);
    }

    private static void createMappingForCountries(SqlService sqlService) {
        //see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
        System.out.println("Creating mapping for countries...");

        String mappingSql = ""
                + "CREATE OR REPLACE MAPPING country("
                + "     __key VARCHAR,"
                + "     isoCode VARCHAR,"
                + "     country VARCHAR"
                + ") TYPE IMap"
                + " OPTIONS ("
                + "     'keyFormat' = 'varchar',"
                + "     'valueFormat' = 'json-flat'"
                + " )";

        try (SqlResult ignored = sqlService.execute(mappingSql)) {
            System.out.println("Mapping for countries has been created");
        }
        System.out.println("--------------------");
    }

    private static void populateCountriesWithMap(HazelcastInstance client) {
        // see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
        System.out.println("Populating 'countries' map with JSON values...");

        IMap<String, HazelcastJsonValue> countries = client.getMap("country");
        countries.put("AU", Country.asJson("AU", "Australia"));
        countries.put("EN", Country.asJson("EN", "England"));
        countries.put("US", Country.asJson("US", "United States"));
        countries.put("CZ", Country.asJson("CZ", "Czech Republic"));

        System.out.println("The 'countries' map has been populated.");
        System.out.println("--------------------");
    }

    private static void selectAllCountries(SqlService sqlService) {
        String sql = "SELECT c.country from country c";
        System.out.println("Select all countries with sql = " + sql);
        try (SqlResult result = sqlService.execute(sql)) {
            result.forEach(row -> System.out.println("country = " + row.getObject("country")));
        }
        System.out.println("--------------------");
    }

    private static void createMappingForCities(SqlService sqlService) {
        //see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
        System.out.println("Creating mapping for cities...");

        String mappingSql = ""
                + "CREATE OR REPLACE MAPPING city("
                + " __key INT ,"
                + " country VARCHAR ,"
                + " city VARCHAR,"
                + " population BIGINT)"
                + " TYPE IMap"
                + " OPTIONS ("
                + "     'keyFormat' = 'int',"
                + "     'valueFormat' = 'json-flat'"
                + " )";

        try (SqlResult ignored = sqlService.execute(mappingSql)) {
            System.out.println("Mapping for cities has been created");
        }
        System.out.println("--------------------");
    }

    private static void populateCities(HazelcastInstance client) {
        // see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
        System.out.println("Populating 'city' map with JSON values...");

        IMap<Integer, HazelcastJsonValue> cities = client.getMap("city");
        cities.put(1, City.asJson("AU", "Canberra", 467_194));
        cities.put(2, City.asJson("CZ", "Prague", 1_318_085));
        cities.put(3, City.asJson("EN", "London", 9_540_576));
        cities.put(4, City.asJson("US", "Washington, DC", 7_887_965));

        System.out.println("The 'city' map has been populated.");
        System.out.println("--------------------");
    }

    private static void selectCitiesByCountry(SqlService sqlService, String country) {
        String sql = "SELECT city, population FROM city where country=?";
        System.out.println("--------------------");
        System.out.println("Select city and population with sql = " + sql);
        try (SqlResult result = sqlService.execute(sql, country)) {
            result.forEach(row ->
                    System.out.printf("city = %s, population = %s%n", row.getObject("city"), row.getObject("population"))
            );
        }
        System.out.println("--------------------");
    }

    private static void selectCountriesAndCities(SqlService sqlService) {
        String sql = ""
                + "SELECT c.isoCode, c.country, t.city, t.population"
                + "  FROM country c"
                + "       JOIN city t ON c.isoCode = t.country";

        System.out.println("Select country and city data in query that joins tables");
        System.out.printf("%4s | %15s | %20s | %15s |%n", "iso", "country", "city", "population");

        try (SqlResult result = sqlService.execute(sql)) {
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
