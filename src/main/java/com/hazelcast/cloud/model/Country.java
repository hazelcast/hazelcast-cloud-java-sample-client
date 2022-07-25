package com.hazelcast.cloud.model;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;

public final class Country {
    public static HazelcastJsonValue asJson(String isoCode, String country) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isoCode", isoCode);
        jsonObject.put("country", country);
        return new HazelcastJsonValue(jsonObject.toString());
    }
}
