package com.hazelcast.cloud.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public final class Country {
    private String isoCode;

    private String country;
}
