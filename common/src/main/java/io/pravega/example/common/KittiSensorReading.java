/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.common;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;


import java.sql.Timestamp;

/**
 * A class for storing a single non-video sensor reading.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KittiSensorReading {
    // Unique ID for this sensor.
    public int car_id;
    // The ID of the camera associated with this sensor.
    public int camera;
    // Event time of this sensor reading. We use Timestamp to have nanosecond precision for high-speed cameras.
    public Timestamp timestamp;
    // latitude of the oxts-unit (deg)
    public double lat;
    // longitude of the oxts-unit (deg)
    public double lon;
    // forward velocity, i.e. parallel to earth-surface (m/s)
    public double vf;
    // leftward velocity, i.e. parallel to earth-surface (m/s)
    public double vl;
    // forward acceleration (m/s^2)
    public double af;
    // leftward acceleration (m/s^2)
    public double al;

    @Override
    public String toString() {
        return "KittiSensorReading{" +
                "car_id=" + car_id +
                ", camera=" + camera +
                ", timestamp=" + timestamp +
                ", lat=" + lat +
                ", lon=" + lon +
                ", vf=" + vf +
                ", vl=" + vl +
                ", af=" + af +
                ", al=" + al +
                '}';
    }
}
