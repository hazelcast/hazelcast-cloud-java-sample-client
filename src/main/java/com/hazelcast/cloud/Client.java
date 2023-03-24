/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cloud;

import java.util.Random;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cloud.jobs.UpperCaseFunction;
import com.hazelcast.cloud.model.City;
import com.hazelcast.cloud.model.Country;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

/**
 * This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
 * <p>
 * See: <a href="https://docs.hazelcast.com/cloud/java-client">https://docs.hazelcast.com/cloud/java-client</a>
 */
public class Client {

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getCloudConfig()
            .setDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN")
            .setEnabled(true);
        config.setProperty("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL");
        config.setClusterName("YOUR_CLUSTER_NAME");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        System.out.println("Connection Successful!");
        try {
            mapExample(client);

            //sqlExample(client);

            //compactSerializationExample(client);

            //nonStopMapExample(client);

            //jetJobExample(client);

        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            client.shutdown();
        }
    }

    /**
     * This example shows how to work with Hazelcast maps.
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void mapExample(HazelcastInstance client) {
        IMap<String, City> cities = client.getMap("cities");
        cities.put("1", new City("United Kingdom", "London", 9_540_576));
        cities.put("2", new City("United Kingdom", "Manchester", 2_770_434));
        cities.put("3", new City("United States", "New York", 19_223_191));
        cities.put("4", new City("United States", "Los Angeles", 3_985_520));
        cities.put("5", new City("Turkey", "Ankara", 5_309_690));
        cities.put("6", new City("Turkey", "Istanbul", 15_636_243));
        cities.put("7", new City("Brazil", "Sao Paulo", 22_429_800));
        cities.put("8", new City("Brazil", "Rio de Janeiro", 13_634_274));

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
        // See: https://docs.hazelcast.com/hazelcast/5.2-snapshot/sql/mapping-to-maps#creating-a-mapping-to-a-map
        System.out.println("Creating a mapping...");

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
     * This example shows how to work with Hazelcast SQL queries via Maps that contains compact serialized values.
     *
     * <ul>
     *     <li>Select single element from a Map</li>
     *     <li>Select data from Map with filtering</li>
     *     <li>Join data from two Maps</li>
     * </ul>
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void compactSerializationExample(HazelcastInstance client) {
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
        // See: https://docs.hazelcast.com/hazelcast/5.2-snapshot/sql/mapping-to-maps#compact-objects
        System.out.println("Creating mapping for countries...");

        String mappingSql = ""
            + "CREATE OR REPLACE MAPPING country("
            + "     __key VARCHAR,"
            + "     isoCode VARCHAR,"
            + "     country VARCHAR"
            + ") TYPE IMap"
            + " OPTIONS ("
            + "     'keyFormat' = 'varchar',"
            + "     'valueFormat' = 'compact',"
            + "     'valueCompactTypeName' = 'country'"
            + " )";

        try (SqlResult ignored = sqlService.execute(mappingSql)) {
            System.out.println("Mapping for countries has been created");
        }

        System.out.println("--------------------");
    }

    private static void populateCountriesWithMap(HazelcastInstance client) {
        // See: https://docs.hazelcast.com/hazelcast/5.2-snapshot/data-structures/creating-a-map
        System.out.println("Populating 'countries' map...");

        IMap<String, Country> countries = client.getMap("country");
        countries.put("AU", new Country("AU", "Australia"));
        countries.put("EN", new Country("EN", "England"));
        countries.put("US", new Country("US", "United States"));
        countries.put("CZ", new Country("CZ", "Czech Republic"));

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
        // See: https://docs.hazelcast.com/hazelcast/5.2-snapshot/sql/mapping-to-maps#compact-objects
        System.out.println("Creating mapping for cities...");

        String mappingSql = ""
            + "CREATE OR REPLACE MAPPING city("
            + " __key INT ,"
            + " country VARCHAR ,"
            + " city VARCHAR,"
            + " population INT)"
            + " TYPE IMap"
            + " OPTIONS ("
            + "     'keyFormat' = 'int',"
            + "     'valueFormat' = 'compact',"
            + "     'valueCompactTypeName' = 'city'"
            + " )";

        try (SqlResult ignored = sqlService.execute(mappingSql)) {
            System.out.println("Mapping for cities has been created");
        }
        System.out.println("--------------------");
    }

    private static void populateCities(HazelcastInstance client) {
        // See: https://docs.hazelcast.com/hazelcast/5.2-snapshot/data-structures/creating-a-map
        System.out.println("Populating 'city' map");

        IMap<Integer, City> cities = client.getMap("city");
        cities.put(1, new City("AU", "Canberra", 467_194));
        cities.put(2, new City("CZ", "Prague", 1_318_085));
        cities.put(3, new City("EN", "London", 9_540_576));
        cities.put(4, new City("US", "Washington, DC", 7_887_965));

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

    /**
     * This example shows how to work with Hazelcast maps, where the map is updated continuously.
     *
     * @param client - a {@link HazelcastInstance} client.
     */
    private static void nonStopMapExample(HazelcastInstance client) {
        System.out.println("Now the map named 'map' will be filled with random entries.");

        IMap<String, String> map = client.getMap("map");
        Random random = new Random();
        int iterationCounter = 0;
        while (true) {
            int randomKey = random.nextInt(100_000);
            try {
                map.put("key-" + randomKey, "value-" + randomKey);
                map.get("key-" + random.nextInt(100_000));
                if (++iterationCounter == 10) {
                    iterationCounter = 0;
                    System.out.println("Current map size: " + map.size());
                }
            }
            catch (IllegalStateException | TargetDisconnectedException ex) {
                //rolling update (cluster can be in passive state) e.g. update custom classes
                ex.printStackTrace();
            }
        }
    }

    /**
     * This example shows how to submit simple Jet job which uses logger as a sink. You will be able to see the results
     * of job execution in the Hazelcast cluster logs.
     *
     * @param client- a {@link HazelcastInstance} client.
     */
    private static void jetJobExample(HazelcastInstance client) {
        // See: https://docs.hazelcast.com/hazelcast/5.2/pipelines/submitting-jobs
        System.out.println("Submitting Jet job");

        BatchSource<String> items = TestSources.items(
            "United States", "Turkey", "United Kingdom", "Poland", "Ukraine"
        );

        Pipeline pipeline = Pipeline.create()
            .readFrom(items)
            .map(new UpperCaseFunction())
            .writeTo(Sinks.logger())
            .getPipeline();

        JobConfig jobConfig = new JobConfig()
            .addClass(UpperCaseFunction.class);

        client.getJet().newJob(pipeline, jobConfig);

        System.out.println("Jet job submitted");
    }

}
