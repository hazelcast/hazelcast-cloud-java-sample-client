package com.hazelcast.cloud.model;

import com.hazelcast.core.HazelcastJsonValue;

public final class City {
    public static HazelcastJsonValue asJson(String country, String city, int population) {
        String jsonString = String.format("{"
                + "\"country\": \"%s\", "
                + "\"city\": \"%s\", "
                + "\"population\": %d}", country, city, population);
        return new HazelcastJsonValue(jsonString);
    }
}
