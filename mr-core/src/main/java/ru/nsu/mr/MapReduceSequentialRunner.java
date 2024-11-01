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
import java.util.*;

public class MapReduceSequentialRunner<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT>
        implements MapReduceRunner<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> {
    private MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job;
    private Configuration configuration;
    private Path outputDirectory;
    private Path mappersOutputPath;

    public MapReduceSequentialRunner() {}

    private void mapperJob(List<Path> filesToMap, int mapperId) throws IOException {
        List<FileSystemSink<KEY_INTER, VALUE_INTER>> sortedFileSinks = new ArrayList<>();
        for (int i = 0; i < configuration.get(ConfigurationOption.REDUCERS_COUNT); ++i) {
            sortedFileSinks.add(
                    new SortedFileSink<>(
                            job.getSerializerInterKey(),
                            job.getSerializerInterValue(),
                            job.getDeserializerInterKey(),
                            job.getDeserializerInterValue(),
                            Files.createFile(
                                    mappersOutputPath.resolve(
                                            "mapper-output-" + mapperId + "-" + i + ".txt")),
                            configuration.get(ConfigurationOption.SORTER_IN_MEMORY_RECORDS),
                            job.getComparator()));
        }
        try (PartitionedFileSink<KEY_INTER, VALUE_INTER> partitionedFileSink =
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

    private void reduceJob(List<Path> mappersOutputFiles, int reducerId) throws IOException {
        List<Iterator<Pair<KEY_INTER, VALUE_INTER>>> fileIterators = new ArrayList<>();
        for (Path mappersOutputFile : mappersOutputFiles) {
            fileIterators.add(
                    new KeyValueFileIterator<>(
                            mappersOutputFile,
                            job.getDeserializerInterKey(),
                            job.getDeserializerInterValue()));
        }

        try (FileSink<KEY_OUT, VALUE_OUT> fileSink =
                        new FileSink<>(
                                job.getSerializerOutKey(),
                                job.getSerializerOutValue(),
                                Files.createFile(
                                        outputDirectory.resolve("output-" + reducerId + ".txt")));
                GroupedKeyValuesIterator<KEY_INTER, VALUE_INTER> groupedIterator =
                        new GroupedKeyValuesIterator<>(
                                new MergedKeyValueIterator<>(fileIterators, job.getComparator()))) {
            while (groupedIterator.hasNext()) {
                Pair<KEY_INTER, Iterator<VALUE_INTER>> currentGroup = groupedIterator.next();
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

    @Override
    public void run(
            MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job,
            List<Path> inputFiles,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory) {
        this.job = job;
        this.configuration = configuration;
        this.outputDirectory = outputDirectory;
        this.mappersOutputPath = mappersOutputDirectory;

        int mappersCount = configuration.get(ConfigurationOption.MAPPERS_COUNT);

        int numberOfProcessedInputFiles = 0;
        for (int i = 0; i < mappersCount; ++i) {
            int inputFilesToProcessCount =
                    (inputFiles.size() - numberOfProcessedInputFiles) / (mappersCount - i);
            List<Path> inputFilesToProcess = new ArrayList<>();
            for (int k = 0; k < inputFilesToProcessCount; ++k) {
                Path inputFileToProcess = inputFiles.get(numberOfProcessedInputFiles + k);
                inputFilesToProcess.add(inputFileToProcess);
            }
            try {
                mapperJob(inputFilesToProcess, i);
            } catch (IOException e) {
                throw new RuntimeException();
            }
            numberOfProcessedInputFiles += inputFilesToProcessCount;
        }
        for (int i = 0; i < configuration.get(ConfigurationOption.REDUCERS_COUNT); ++i) {
            List<Path> interFilesToReduce = new ArrayList<>();
            for (int k = 0; k < mappersCount; ++k) {
                interFilesToReduce.add(
                        mappersOutputPath.resolve("mapper-output-" + k + "-" + i + ".txt"));
            }
            try {
                reduceJob(interFilesToReduce, i);
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }
}
