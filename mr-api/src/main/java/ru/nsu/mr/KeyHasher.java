package ru.nsu.mr;

public interface KeyHasher<T> {
    int hash(T key);
}
