package com.hazelcast.cloud;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.impl.discovery.HazelcastCloudDiscovery;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.Properties;
import java.util.Random;

/**
 * This is boilerplate application that configures client to connect Hazelcast Cloud cluster.
 * After successful connection, it puts random entries into the map.
 * <p>
 * See: <a href="https://docs.cloud.hazelcast.com/docs/java-client">https://docs.cloud.hazelcast.com/docs/java-client</a>
 */
public class ClientWithSsl {

    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = ClientWithSsl.class.getClassLoader();
        Properties props = new Properties();
        props.setProperty("javax.net.ssl.keyStore", classLoader.getResource("client.keystore").toURI().getPath());
        props.setProperty("javax.net.ssl.keyStorePassword", "YOUR_SSL_PASSWORD");
        props.setProperty("javax.net.ssl.trustStore", classLoader.getResource("client.truststore").toURI().getPath());
        props.setProperty("javax.net.ssl.trustStorePassword", "YOUR_SSL_PASSWORD");
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));
        config.setGroupConfig(new GroupConfig("YOUR_CLUSTER_NAME", "YOUR_CLUSTER_PASSWORD"));
        config.setProperty("hazelcast.client.statistics.enabled", "true");
        config.setProperty(ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), "YOUR_CLUSTER_DISCOVERY_TOKEN");
        config.setProperty(HazelcastCloudDiscovery.CLOUD_URL_BASE_PROPERTY.getName(), "YOUR_DISCOVERY_URL");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        System.out.println("Connection Successful!");
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
