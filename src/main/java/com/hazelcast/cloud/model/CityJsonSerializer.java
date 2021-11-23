package com.hazelcast.cloud.model;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;

public class CityJsonSerializer {

    public static HazelcastJsonValue cityAsJson(City city) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("country", city.getCountry());
        jsonObject.put("city", city.getCity());
        jsonObject.put("population", city.getPopulation());
        return new HazelcastJsonValue(jsonObject.toString());
    }

}
