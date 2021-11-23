package com.hazelcast.cloud.model;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;

public class CountryJsonSerializer {

    public static HazelcastJsonValue countryAsJson(Country country) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("isoCode", country.getIsoCode());
        jsonObject.put("country", country.getCountry());
        return new HazelcastJsonValue(jsonObject.toString());
    }

}
