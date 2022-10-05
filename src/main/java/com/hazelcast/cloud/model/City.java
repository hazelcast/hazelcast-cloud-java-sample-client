package com.hazelcast.cloud.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public final class City {
    private String country;

    private String city;

    private int population;
}
