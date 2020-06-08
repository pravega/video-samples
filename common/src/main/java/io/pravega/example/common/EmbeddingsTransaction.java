package io.pravega.example.common;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
 *   Stores and serializes JPEG image data with their calculated embeddings.
 * */

@JsonIgnoreProperties(ignoreUnknown = true)
public class EmbeddingsTransaction extends Transaction {
    public List<float[]> embeddingValues;


    public EmbeddingsTransaction() {
        super();
        embeddingValues = new ArrayList<float[]>();
    }

    public EmbeddingsTransaction(String personId, String imageName, byte[] imageData, String transactionType, Timestamp timestamp, List<float[]> embeddingValues) {
        super(personId, imageName, imageData, transactionType, timestamp);
        this.embeddingValues = embeddingValues;
    }

    public List<float[]> getEmbeddingValues() { return this.embeddingValues; }
}
