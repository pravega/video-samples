package io.pravega.example.common;

import java.sql.Timestamp;

/*
 *   This class represents a single facial embedding of a person using the algorithm.
 * */

public class Embedding {
    public String personId;
    public float[] embeddingValue;
    public String imageName; // Identifier of images used to produce embedding
    public Timestamp timestamp; // Time at which this embedding was captured to be added to database

    public Embedding(String personId, float[] embeddingValue, String imageName, Timestamp timestamp) {
        this.personId = personId;
        this.embeddingValue = embeddingValue;
        this.imageName = imageName;
        this.timestamp = timestamp;
    }

    public String getPersonId() {
        return this.personId;
    }

    public float[] getEmbeddingValue() {
        return this.embeddingValue;
    }

    public String getImageName() {
        return this.imageName;
    }

    public Timestamp getTimestamp() {
        return this.timestamp;
    }

    public String toString() {
        return "personId={" + this.personId + "}" +
                ", imageName={" + this.imageName + "}" +
                ", timestamp={" + this.timestamp.toGMTString() + "}";
    }
}
