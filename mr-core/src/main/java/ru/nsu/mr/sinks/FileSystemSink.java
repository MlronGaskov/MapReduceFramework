package ru.nsu.mr.sinks;

import java.io.IOException;

public interface FileSystemSink<K, V> extends AutoCloseable {
    void put(K key, V value) throws IOException;

    @Override
    void close() throws IOException;
}
