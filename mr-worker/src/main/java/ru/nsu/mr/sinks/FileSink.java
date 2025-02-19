package ru.nsu.mr.sinks;

import ru.nsu.mr.Serializer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSink<K, V> implements FileSystemSink<K, V> {
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final BufferedWriter writer;

    public FileSink(Serializer<K> keySerializer, Serializer<V> valueSerializer, Path outputPath)
            throws IOException {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.writer = Files.newBufferedWriter(outputPath);
    }

    @Override
    public void put(K key, V value) throws IOException {
        String keyString = keySerializer.serialize(key);
        String valueString = valueSerializer.serialize(value);
        writer.write(keyString + " " + valueString);
        writer.newLine();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
