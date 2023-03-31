package com.hazelcast.cloud;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cloud.model.City;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static com.hazelcast.cloud.ClientWithSsl.*;

public class JdbcClient {

    public static void main(String[] args) throws URISyntaxException {
        // Configure the client
        ClassLoader classLoader = JdbcClient.class.getClassLoader();
        Properties tlsProperties = new Properties();
        tlsProperties.setProperty("javax.net.ssl.keyStore", classLoader.getResource("client.keystore").toURI().getPath());
        tlsProperties.setProperty("javax.net.ssl.keyStorePassword", "YOUR_SSL_PASSWORD");
        tlsProperties.setProperty("javax.net.ssl.trustStore",
                classLoader.getResource("client.truststore").toURI().getPath());
        tlsProperties.setProperty("javax.net.ssl.trustStorePassword", "YOUR_SSL_PASSWORD");

        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(tlsProperties));
        config.getNetworkConfig().getCloudConfig()
                .setDiscoveryToken("YOUR_CLUSTER_DISCOVERY_TOKEN")
                .setEnabled(true);
        config.setProperty("hazelcast.client.cloud.url", "YOUR_DISCOVERY_URL");
        config.setClusterName("YOUR_CLUSTER_NAME");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        // prepare the data
        createMapping(client.getSql());
        insertCities(client);

        // fetch the data through JDBC
        fetchDataThroughJdbc(tlsProperties, config);
    }

    private static void fetchDataThroughJdbc(Properties tlsProperties, ClientConfig config) {
        String jdbcUrl = String.format("jdbc:hazelcast://%s/?discoveryToken=%s&cloudUrl=%s&sslEnabled=true",
                config.getClusterName(),
                config.getNetworkConfig().getCloudConfig().getDiscoveryToken(),
                config.getProperties().getProperty("hazelcast.client.cloud.url"));

        try (Connection connection = DriverManager.getConnection(jdbcUrl, tlsProperties);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT this FROM cities")) {
            System.out.println("--Fetching the results through JDBC interface:");
            while (rs.next()) {
                City c = (City) rs.getObject("this");
                System.out.println(String.format("City: %s, Population: %s", c.getCity(), c.getPopulation()));
            }
        } catch (Exception e) {
            System.out.println("An error occurred while using the JDBC driver: " + e.getMessage());
        }
    }
}
