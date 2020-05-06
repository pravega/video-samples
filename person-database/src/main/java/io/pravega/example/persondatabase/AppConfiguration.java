package io.pravega.example.persondatabase;

import io.pravega.example.common.PravegaAppConfiguration;

public class AppConfiguration extends PravegaAppConfiguration {
    private final String personId;
    private final String imagePath;
    private final String transactionType;

    public AppConfiguration(String[] args) {
        super(args);
        personId = getEnvVar("personId", "");
        imagePath = getEnvVar("imagePath", "");
        transactionType = getEnvVar("transactionType", "");
    }

    public String getpersonId() {
        return personId;
    }

    public String getimagePath() {
        return imagePath;
    }

    public String gettransactionType() {
        return transactionType;
    }

    @Override
    public String toString() {
        return "AppConfiguration{" +
                super.toString() +
                ", personId=" + personId +
                ", imagePath=" + imagePath +
                ", transactionType=" + transactionType +
                '}';
    }
}
