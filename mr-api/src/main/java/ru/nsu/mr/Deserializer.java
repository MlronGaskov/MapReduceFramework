package ru.nsu.mr;

public interface Deserializer<T> {
    T deserialize(String input);
}
