package com.hazelcast.cloud.model;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

public class CitySerializer implements CompactSerializer<City> {
    @Override
    public City read(CompactReader compactReader) {
        return new City(compactReader.readString("country"),
                        compactReader.readString("city"),
                        compactReader.readInt32("population"));
    }

    @Override
    public void write(CompactWriter compactWriter, City city) {
        compactWriter.writeString("country", city.getCountry());
        compactWriter.writeString("city", city.getCity());
        compactWriter.writeInt32("population", city.getPopulation());
    }

    @Override
    public String getTypeName() {
        return "city";
    }

    @Override
    public Class<City> getCompactClass() {
        return City.class;
    }
}
