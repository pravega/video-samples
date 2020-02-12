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
package io.pravega.example.common;

import java.sql.Timestamp;

/**
 * A class for storing a single non-video sensor reading.
 */
public class SensorReading {
    // Unique ID for this sensor.
    public int sensorId;
    // The ID of the camera associated with this sensor.
    public int camera;
    // Event time of this sensor reading. We use Timestamp to have nanosecond precision for high-speed cameras.
    public Timestamp timestamp;
    public double temperatureCelsius;
    // Relative humidity as a percentage between 0 and 100.
    public double humidity;

    @Override
    public String toString() {
        return "SensorReading{" +
                "sensorId=" + sensorId +
                ", camera=" + camera +
                ", timestamp=" + timestamp +
                ", temperatureCelsius=" + temperatureCelsius +
                ", humidity=" + humidity +
                '}';
    }
}
