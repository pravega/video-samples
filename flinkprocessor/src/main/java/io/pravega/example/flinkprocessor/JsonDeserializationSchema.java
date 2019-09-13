/*
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.flinkprocessor;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Deserializes a Java class from JSON.
 */
public class JsonDeserializationSchema<T> extends AbstractDeserializationSchema<T> {
    private final Class<T> valueType;
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public JsonDeserializationSchema(Class<T> valueType) {
        super(valueType);
        this.valueType = valueType;
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, valueType);
    }
}
