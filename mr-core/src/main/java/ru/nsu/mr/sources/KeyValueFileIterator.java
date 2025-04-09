package ru.nsu.mr.sources;

import ru.nsu.mr.Deserializer;
import ru.nsu.mr.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class KeyValueFileIterator<K, V> implements Iterator<Pair<K, V>>, AutoCloseableSource {
    private final BufferedReader reader;
    private String nextLine;

    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public KeyValueFileIterator(
            Path filePath, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer)
            throws IOException {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;

        if (filePath.toString().endsWith(".zip")) {
            ZipInputStream zipInputStream = new ZipInputStream(Files.newInputStream(filePath));
            ZipEntry entry = zipInputStream.getNextEntry();
            if (entry == null) {
                throw new IOException("ZIP file is empty: " + filePath);
            }

            InputStreamReader isr = new InputStreamReader(zipInputStream);
            this.reader = new BufferedReader(isr);
        } else {
            this.reader = Files.newBufferedReader(filePath);
        }

        this.nextLine = readNextLine();
    }

    private String readNextLine() throws IOException {
        return reader.readLine();
    }

    @Override
    public boolean hasNext() {
        return nextLine != null;
    }

    @Override
    public Pair<K, V> next() {
        if (nextLine == null) {
            throw new NoSuchElementException();
        }

        String[] parts = nextLine.split(" ", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid key-value pair: " + nextLine);
        }

        K key = keyDeserializer.deserialize(parts[0]);
        V value = valueDeserializer.deserialize(parts[1]);
        try {
            nextLine = readNextLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new Pair<>(key, value);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
