package io.pravega.example.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 *   This class represents the schema for interacting with the Embeddings database
 * */

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {
    public String personId;
    public String imageName;
    public byte[] imageData;
    public String transactionType;
    public Timestamp timestamp;
    public List<float[]> embeddingValues = new ArrayList<float[]>();


    public Transaction() {

    }

    public Transaction(String personId, String imageName, byte[] imageData, String transactionType, Timestamp timestamp, List<float[]> embeddingValues) {
        this.personId = personId;
        this.imageName = imageName;
        this.imageData = imageData;
        this.transactionType = transactionType;
        this.timestamp = timestamp;
        this.embeddingValues = embeddingValues;
    }

    public String getPersonId() {
        return this.personId;
    }

    public String getImageName() {
        return this.imageName;
    }

    public byte[] getImageData() {
        return this.imageData;
    }

    public String getTransactionType() {
        return this.transactionType;
    }

    public Timestamp getTimestamp() {
        return this.timestamp;
    }

    public List<float[]> getEmbeddingValues() { return this.embeddingValues; }

    public String toString() {
        String dataStr = "null";
        int dataLength = 0;

        // crop the data
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
                "timestamp={" + timestamp.toGMTString() + "}, ";
    }
}
