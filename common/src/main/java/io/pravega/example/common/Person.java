package io.pravega.example.common;

public class Person {
    public String id;
//    public String name;
    public float[] embedding;
    public Person() {
        this("Unknown", new float[1]);
    }

    public Person(String id, float[] embedding) {
        this.id = id;
        this.embedding = embedding;
    }
}
