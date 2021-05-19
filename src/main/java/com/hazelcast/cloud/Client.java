package com.hazelcast.cloud;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudDiscovery;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.Random;

/**
 * This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
 * After successful connection, it puts random entries into the map.
 * <p>
 * See: <a href="https://docs.cloud.hazelcast.com/docs/java-client">https://docs.cloud.hazelcast.com/docs/java-client</a>
 */
public class Client {

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        config.setGroupConfig(new GroupConfig("YOUR_CLUSTER_NAME", "YOUR_CLUSTER_PASSWORD"));
        config.setProperty("hazelcast.client.statistics.enabled", "true");
        config.setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "YOUR_CLUSTER_DISCOVERY_TOKEN");
        config.setProperty(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY.getName(), "YOUR_DISCOVERY_URL");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        System.out.println("Connection Successful!");
        System.out.println("Now the map named 'map' will be filled with random entries.");

        IMap<String, String> map = client.getMap("map");
        Random random = new Random();
        byte iterationCounter = 0;
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
