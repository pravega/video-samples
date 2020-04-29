package io.pravega.example.tensorflow;

public class Person {
    public int id;
    public String name;
    public float[] embedding;
    public Person() {
        this(-1,"Unknown", new float[1]);
    }

    public Person(int id, String name, float[] embedding) {
        this.id = id;
        this.name = name;
        this.embedding = embedding;
    }
}
