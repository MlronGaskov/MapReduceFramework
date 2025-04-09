package ru.nsu.mr.sinks;

import ru.nsu.mr.Serializer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFileSink<K, V> implements FileSystemSink<K, V> {
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final BufferedWriter writer;
    private final ZipOutputStream zipOutputStream;

    public ZipFileSink(Serializer<K> keySerializer, Serializer<V> valueSerializer, Path zipPath)
            throws IOException {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.zipOutputStream = new ZipOutputStream(Files.newOutputStream(zipPath));

        String entryName = zipPath.getFileName().toString().replaceFirst("\\.zip$", ".txt");
        zipOutputStream.putNextEntry(new ZipEntry(entryName));

        this.writer = new BufferedWriter(new OutputStreamWriter(zipOutputStream));
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
        writer.flush();
        zipOutputStream.closeEntry(); // нужно закрыть entry до writer
        writer.close();
    }
}

