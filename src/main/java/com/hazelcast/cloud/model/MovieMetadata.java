package com.hazelcast.cloud.model;

import java.io.Serializable;

public class MovieMetadata implements Serializable {
    String name;
    String releaseDate;
    String plotSummary;

    public MovieMetadata(String name, String releaseDate, String plotSummary) {
        this.name = name;
        this.releaseDate = releaseDate;
        this.plotSummary = plotSummary;
    }

    public String getName() {
        return name;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public String getPlotSummary() {
        return plotSummary;
    }
}
