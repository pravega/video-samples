package io.pravega.example.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {
    public String personId;
    public String imageName;
    public byte[] imageData;
    public String transactionType;
    public Timestamp timestamp;


    public Transaction() {

    }

    public Transaction(String personId, String imageName, byte[] imageData, String transactionType, Timestamp timestamp) {
        this.personId = personId;
        this.imageName = imageName;
        this.imageData = imageData;
        this.transactionType = transactionType;
        this.timestamp = timestamp;
    }

    @JsonProperty("personId")
    public String getPersonId() {
        return this.personId;
    }

    @JsonProperty("imageName")
    public String getImageName() {
        return this.imageName;
    }

    @JsonProperty("imageData")
    public byte[] getImageData() {
        return this.imageData;
    }

    @JsonProperty("transactionType")
    public String getTransactionType() {
        return this.transactionType;
    }

    @JsonProperty("timestamp")
    public Timestamp getTimestamp() {
        return this.timestamp;
    }

    public String toString() {
        String dataStr = "null";
        int dataLength = 0;
        if (this.imageData != null) {
            dataLength = this.imageData.length;
            int sizeToPrint = dataLength;
            int maxSizeToPrint = 10;
            if (sizeToPrint > maxSizeToPrint) {
                sizeToPrint = maxSizeToPrint;
            }
            dataStr = Arrays.toString(Arrays.copyOf(this.imageData, sizeToPrint));
        }
        return "personId={" + personId + "}, " +
                "imageName={" + imageName + "}, " +
                "transactionType={" + transactionType + "}, " +
                "data(" + dataLength + ")={" + dataStr + "}, " +
                "timestamp={" + timestamp.toGMTString() + "}";
    }
}
