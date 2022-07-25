package com.hazelcast.cloud.model;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;

public final class City {
    public static HazelcastJsonValue asJson(String country, String city, int population) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("country", country);
        jsonObject.put("city", city);
        jsonObject.put("population", population);
        return new HazelcastJsonValue(jsonObject.toString());
    }
}
