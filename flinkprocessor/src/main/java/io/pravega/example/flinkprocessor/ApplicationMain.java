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
