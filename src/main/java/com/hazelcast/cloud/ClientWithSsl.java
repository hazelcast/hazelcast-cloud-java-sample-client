package com.hazelcast.cloud;

import java.util.Properties;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cloud.jobs.UpperCaseFunction;
import com.hazelcast.cloud.model.User;
import com.hazelcast.cloud.model.UserSerializer;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

/**
 * This is boilerplate application that configures client to connect Hazelcast Viridian cluster.
 * <p>
 * See: <a href="https://docs.hazelcast.com/cloud/java-client">https://docs.hazelcast.com/cloud/java-client</a>
 */
public class ClientWithSsl {

    public static void main(String[] args) throws Exception {

        // Configure the client.
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

        config.getSerializationConfig().getCompactSerializationConfig().addSerializer(new UserSerializer());

        System.out.println("Connect Hazelcast Viridian with TLS");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        System.out.println("Connection Successful!");

        try {
            createMapping(client.getSql());
            insertUsers(client);
            fetchUsersWithSQL(client.getSql());
            jetJobExample(client);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            client.shutdown();
        }
    }

    private static void createMapping(SqlService sqlService) {
        // See: https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-maps#compact-objects
        System.out.println("Creating mapping for users...");

        String mappingSql = ""
            + "CREATE OR REPLACE MAPPING users("
            + "     __key INT,"
            + "     name VARCHAR,"
            + "     country VARCHAR"
            + ") TYPE IMap"
            + " OPTIONS ("
            + "     'keyFormat' = 'int',"
            + "     'valueFormat' = 'compact',"
            + "     'valueCompactTypeName' = 'user'"
            + " )";

        try (SqlResult ignored = sqlService.execute(mappingSql)) {
            System.out.print("OK.");
        }
    }

    private static void insertUsers(HazelcastInstance client){
        System.out.print("\nInserting users into 'users' map...");

        String insertQuery = "INSERT INTO users "
                +"(__key, name, country) VALUES"
                +"(1, 'Emre', 'Türkiye'),"
                +"(2, 'Aika', 'Japan'),"
                +"(3, 'John', 'United States'),"
                +"(4, 'Olivia', 'United Kingdom'),"
                +"(5, 'Jonas', 'Germany')";

        try{
            SqlResult result = client.getSql().execute(insertQuery);
            System.out.print("Inserted...");
        }catch (Exception ex){
            System.out.println(ex.getMessage());
        }

        // Let's also add a user as object.
        IMap<Integer, User> map = client.getMap("users");

        User u = new User();
        u.setName("Alice");
        u.setCountry("Brazil");
        map.putAsync(6, u);

        System.out.print("OK.");
    }

    private static void fetchUsersWithSQL(SqlService sqlService) {

        System.out.println("\nFetching users via SQL...");

        try(SqlResult result = sqlService.execute("SELECT __key, this FROM users")) {

            System.out.println("--Results of 'SELECT __key, this FROM users'");

            for (SqlRow row : result) {
                int id = row.getObject("__key");
                User u = row.getObject("this");
                System.out.println("Id:" + id + "\tName:" + u.getName() + "\tCountry:" + u.getCountry());
            }
        }

        System.out.println("\n!! Hint !! You can execute your SQL queries on your Viridian cluster over the management center." +
                "\n 1. Go to 'Management Center' of your Hazelcast Viridian cluster. \n 2. Open the 'SQL Browser'. \n 3. Try to execute 'SELECT * FROM users'.\n");
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
            "United States", "Türkiye", "United Kingdom", "Poland", "Ukraine"
        );

        Pipeline pipeline = Pipeline.create()
            .readFrom(items)
            .map(new UpperCaseFunction())
            .writeTo(Sinks.logger()) // Results will be visible on the server logs.
            .getPipeline();

        JobConfig jobConfig = new JobConfig()
            .addClass(UpperCaseFunction.class);

        client.getJet().newJob(pipeline, jobConfig);

        System.out.println("Jet job submitted. \nYou can see the results in the logs. Go to your Viridian cluster, and click the 'Logs'.");
    }

}
