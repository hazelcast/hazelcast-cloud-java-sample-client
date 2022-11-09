package com.hazelcast.cloud.jobs;

import com.hazelcast.function.FunctionEx;

public class UpperCaseFunction implements FunctionEx<String, String> {
    @Override
    public String applyEx(String s) {
        return s.toUpperCase();
    }
}
