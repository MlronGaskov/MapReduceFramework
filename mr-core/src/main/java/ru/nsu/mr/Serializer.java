package ru.nsu.mr;

@FunctionalInterface
public interface Serializer<T> {
    String serialize(T input);
}
