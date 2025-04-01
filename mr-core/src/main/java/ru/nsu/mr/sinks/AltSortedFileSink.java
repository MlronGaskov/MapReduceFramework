package ru.nsu.mr.sinks;

import ru.nsu.mr.Deserializer;
import ru.nsu.mr.Pair;
import ru.nsu.mr.Reducer;
import ru.nsu.mr.Serializer;
import ru.nsu.mr.sources.KeyValueFileIterator;
import ru.nsu.mr.sources.MergedKeyValueIterator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class AltSortedFileSink<K1, V1, K2, V2> implements FileSystemSink<K1, V1> {
    private final Serializer<K2> outKeySerializer;
    private final Serializer<V2> outValueSerializer;
    private final Deserializer<K2> outKeyDeserializer;
    private final Deserializer<V2> outValueDeserializer;
    private final Comparator<K2> comparator;
    private final Map<K1, List<V1>> buffer;
    private final int bufferMaxSize;
    private final Path outputPath;
    private final List<Path> dumps;
    private final Reducer<K1, V1, K2, V2> reducer;
    private int curBufferSize;

    public AltSortedFileSink(
            Serializer<K2> outKeySerializer,
            Serializer<V2> outValueSerializer,
            Deserializer<K2> outKeyDeserializer,
            Deserializer<V2> outValueDeserializer,
            Path outputPath,
            int bufferMaxSize,
            Comparator<K2> comparator,
            Reducer<K1, V1, K2, V2> reducer) {
        this.outKeySerializer = outKeySerializer;
        this.outValueSerializer = outValueSerializer;
        this.outKeyDeserializer = outKeyDeserializer;
        this.outValueDeserializer = outValueDeserializer;
        this.outputPath = outputPath;
        this.buffer = new HashMap<>();
        this.bufferMaxSize = bufferMaxSize;
        this.comparator = comparator;
        this.dumps = new ArrayList<>();
        this.reducer = reducer;
        this.curBufferSize = 0;
    }

    @Override
    public void put(K1 key, V1 value) throws IOException {
        buffer.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        curBufferSize++;
        if (curBufferSize >= bufferMaxSize) {
            flushBuffer();
        }
    }

    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        List<Pair<K2, V2>> reducedBuffer = new ArrayList<>();

        for (Map.Entry<K1, List<V1>> entry : buffer.entrySet()) {
            reducer.reduce(
                    entry.getKey(),
                    entry.getValue().iterator(),
                    (k2, v2) -> reducedBuffer.add(new Pair<>(k2, v2))
            );
        }

        reducedBuffer.sort(Comparator.comparing(Pair::key, comparator));
        dumps.add(Files.createTempFile("dump_" + dumps.size(), ".txt"));
        try (FileSink<K2, V2> tempSink =
                     new FileSink<>(outKeySerializer, outValueSerializer, dumps.getLast())) {
            for (Pair<K2, V2> pair : reducedBuffer) {
                tempSink.put(pair.key(), pair.value());
            }
        }
        buffer.clear();
    }

    @Override
    public void close() throws IOException {
        flushBuffer();

        List<Iterator<Pair<K2, V2>>> dumpsIterators = new ArrayList<>();
        for (Path dump : dumps) {
            KeyValueFileIterator<K2, V2> dumpIterator;
            dumpIterator = new KeyValueFileIterator<>(dump, outKeyDeserializer, outValueDeserializer);
            dumpsIterators.add(dumpIterator);
        }

        try (FileSink<K2, V2> outputFileSink =
                     new FileSink<>(outKeySerializer, outValueSerializer, outputPath);
             MergedKeyValueIterator<K2, V2> mergedDumps =
                     new MergedKeyValueIterator<>(dumpsIterators, comparator)) {
            while (mergedDumps.hasNext()) {
                Pair<K2, V2> KeyValue = mergedDumps.next();
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
