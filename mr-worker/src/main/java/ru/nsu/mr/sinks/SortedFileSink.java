package ru.nsu.mr.sinks;

import ru.nsu.mr.Deserializer;
import ru.nsu.mr.Pair;
import ru.nsu.mr.Serializer;
import ru.nsu.mr.sources.KeyValueFileIterator;
import ru.nsu.mr.sources.MergedKeyValueIterator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SortedFileSink<K, V> implements FileSystemSink<K, V> {
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Comparator<K> comparator;
    private final List<Pair<K, V>> buffer;
    private final int bufferSize;
    private final Path outputPath;
    private final List<Path> dumps;

    public SortedFileSink(
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            Path outputPath,
            int bufferSize,
            Comparator<K> comparator) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.outputPath = outputPath;
        this.buffer = new ArrayList<>();
        this.bufferSize = bufferSize;
        this.comparator = comparator;
        this.dumps = new ArrayList<>();
    }

    @Override
    public void put(K key, V value) throws IOException {
        buffer.add(new Pair<>(key, value));
        if (buffer.size() >= bufferSize) {
            flushBuffer();
        }
    }

    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        buffer.sort(Comparator.comparing(Pair::key, comparator));
        dumps.add(Files.createTempFile("dump_" + dumps.size(), ".txt"));
        try (FileSink<K, V> tempSink =
                new FileSink<>(keySerializer, valueSerializer, dumps.getLast())) {
            for (Pair<K, V> pair : buffer) {
                tempSink.put(pair.key(), pair.value());
            }
        }
        buffer.clear();
    }

    @Override
    public void close() throws IOException {
        flushBuffer();

        List<Iterator<Pair<K, V>>> dumpsIterators = new ArrayList<>();
        for (Path dump : dumps) {
            KeyValueFileIterator<K, V> dumpIterator;
            dumpIterator = new KeyValueFileIterator<>(dump, keyDeserializer, valueDeserializer);
            dumpsIterators.add(dumpIterator);
        }

        try (FileSink<K, V> outputFileSink =
                        new FileSink<>(keySerializer, valueSerializer, outputPath);
                MergedKeyValueIterator<K, V> mergedDumps =
                        new MergedKeyValueIterator<>(dumpsIterators, comparator)) {
            while (mergedDumps.hasNext()) {
                Pair<K, V> KeyValue = mergedDumps.next();
                outputFileSink.put(KeyValue.key(), KeyValue.value());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            for (Path dump : dumps) {
                Files.deleteIfExists(dump);
            }
        }
    }
}
