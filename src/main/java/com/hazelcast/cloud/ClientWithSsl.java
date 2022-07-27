package com.hazelcast.cloud;

import java.util.Properties;
import java.util.Random;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cloud.model.City;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;

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

        //nonStopMapExample(client);

        client.shutdown();

        System.exit(0);
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
     * This example shows how to work with Hazelcast maps, where the map is
     * updated continuously.
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
            map.put("key-" + randomKey, "value-" + randomKey);
            map.get("key-" + random.nextInt(100_000));
            if (++iterationCounter == 10) {
                iterationCounter = 0;
                System.out.println("Current map size: " + map.size());
            }
        }
    }
}
