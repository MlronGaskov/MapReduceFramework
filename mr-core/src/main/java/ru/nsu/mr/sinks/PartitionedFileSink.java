package ru.nsu.mr.sinks;

import ru.nsu.mr.KeyHasher;

import java.io.IOException;
import java.util.List;

public class PartitionedFileSink<K, V> implements FileSystemSink<K, V> {
    private final List<FileSystemSink<K, V>> sinks;
    private final KeyHasher<K> hasher;

    public PartitionedFileSink(List<FileSystemSink<K, V>> sinks, KeyHasher<K> hasher) {
        if (sinks == null || sinks.isEmpty()) {
            throw new IllegalArgumentException("Sink list cannot be null or empty.");
        }
        this.sinks = sinks;
        this.hasher = hasher;
    }

    @Override
    public void put(K key, V value) throws IOException {
        sinks.get(getSinkToPutIndex(key)).put(key, value);
    }

    @Override
    public void close() throws IOException {
        for (FileSystemSink<K, V> sink : sinks) {
            sink.close();
        }
    }

    private int getSinkToPutIndex(K key) {
        int hash = hasher.hash(key);
        return (hash % sinks.size() + sinks.size()) % sinks.size();
    }
}
