package ru.nsu.mr;

public interface OutputContext<K, V> {
    void put(K key, V value);
}
