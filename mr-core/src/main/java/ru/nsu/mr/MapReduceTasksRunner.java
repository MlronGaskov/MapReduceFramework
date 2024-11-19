package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.sinks.FileSink;
import ru.nsu.mr.sinks.FileSystemSink;
import ru.nsu.mr.sinks.PartitionedFileSink;
import ru.nsu.mr.sinks.SortedFileSink;
import ru.nsu.mr.sources.GroupedKeyValuesIterator;
import ru.nsu.mr.sources.KeyValueFileIterator;
import ru.nsu.mr.sources.MergedKeyValueIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapReduceTasksRunner {
    public static <K_I, V_I, K_O, V_O> void executeMapperTask(
            List<Path> filesToMap,
            int mapperId,
            Path mappersOutputDirectory,
            Configuration configuration,
            MapReduceJob<K_I, V_I, K_O, V_O> job)
            throws IOException {
        List<FileSystemSink<K_I, V_I>> sortedFileSinks = new ArrayList<>();
        for (int i = 0; i < configuration.get(ConfigurationOption.REDUCERS_COUNT); ++i) {
            sortedFileSinks.add(
                    new SortedFileSink<>(
                            job.getSerializerInterKey(),
                            job.getSerializerInterValue(),
                            job.getDeserializerInterKey(),
                            job.getDeserializerInterValue(),
                            Files.createFile(
                                    mappersOutputDirectory.resolve(
                                            "mapper-output-" + mapperId + "-" + i + ".txt")),
                            configuration.get(ConfigurationOption.SORTER_IN_MEMORY_RECORDS),
                            job.getComparator()));
        }
        try (PartitionedFileSink<K_I, V_I> partitionedFileSink =
                new PartitionedFileSink<>(sortedFileSinks, job.getHasher())) {
            for (Path inputFileToProcess : filesToMap) {
                BufferedReader reader = Files.newBufferedReader(inputFileToProcess);
                String line = reader.readLine();

                Iterator<Pair<String, String>> iterator =
                        new Iterator<>() {
                            String nextLine = line;

                            @Override
                            public boolean hasNext() {
                                return nextLine != null;
                            }

                            @Override
                            public Pair<String, String> next() {
                                if (!hasNext()) {
                                    throw new RuntimeException();
                                }
                                String currentLine = nextLine;
                                try {
                                    nextLine = reader.readLine();
                                } catch (IOException e) {
                                    throw new RuntimeException();
                                }
                                return new Pair<>(inputFileToProcess.toString(), currentLine);
                            }
                        };

                job.getMapper()
                        .map(
                                iterator,
                                (outputKey, outputValue) -> {
                                    try {
                                        partitionedFileSink.put(outputKey, outputValue);
                                    } catch (IOException e) {
                                        throw new RuntimeException();
                                    }
                                });
            }
        }
    }

    public static <K_I, V_I, K_O, V_O> void executeReduceTask(
            List<Path> mappersOutputFiles,
            int reducerId,
            Path outputDirectory,
            Configuration configuration,
            MapReduceJob<K_I, V_I, K_O, V_O> job)
            throws IOException {
        List<Iterator<Pair<K_I, V_I>>> fileIterators = new ArrayList<>();
        for (Path mappersOutputFile : mappersOutputFiles) {
            fileIterators.add(
                    new KeyValueFileIterator<>(
                            mappersOutputFile,
                            job.getDeserializerInterKey(),
                            job.getDeserializerInterValue()));
        }

        try (FileSink<K_O, V_O> fileSink =
                        new FileSink<>(
                                job.getSerializerOutKey(),
                                job.getSerializerOutValue(),
                                Files.createFile(
                                        outputDirectory.resolve("output-" + reducerId + ".txt")));
                GroupedKeyValuesIterator<K_I, V_I> groupedIterator =
                        new GroupedKeyValuesIterator<>(
                                new MergedKeyValueIterator<>(fileIterators, job.getComparator()))) {
            while (groupedIterator.hasNext()) {
                Pair<K_I, Iterator<V_I>> currentGroup = groupedIterator.next();
                job.getReducer()
                        .reduce(
                                currentGroup.key(),
                                currentGroup.value(),
                                (outputKey, outputValue) -> {
                                    try {
                                        fileSink.put(outputKey, outputValue);
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
            }
        }
    }
}