package io.pravega.example.common;

/*
 *   This holds relevant information about the people in the Embeddings database.
 * */

public class Person {
    public String id;

    public float[] embedding;

    public Person() {
        this("Unknown", new float[1]);
    }

    public Person(String id, float[] embedding) {
        this.id = id;
        this.embedding = embedding;
    }
}
