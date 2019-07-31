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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMain {
    private static Logger log = LoggerFactory.getLogger(ApplicationMain.class);

    public static void main(String... args) throws Exception {
        // Load configuration that is used by all jobs.
        AppConfiguration appConfiguration = new AppConfiguration(args);
        String jobClassName = appConfiguration.getJobClass();
        // Dynamically load job class and run it.
        Class<?> jobClass = Class.forName(jobClassName);
        AbstractJob job = (AbstractJob) jobClass.getConstructor(AppConfiguration.class).newInstance(appConfiguration);
        job.run();
    }
}
