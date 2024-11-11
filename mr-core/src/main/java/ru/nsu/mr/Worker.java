package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.*;
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

public class Worker<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> {
    private MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job;
    private Configuration configuration;
    private Path outputDirectory;
    private Path mappersOutputPath;
    private final WorkerManager workerManager;
    private final WorkerEndpoint workerEndpoint;

    public Worker(
            MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory,
            int serverPort)
            throws IOException {
        this.job = job;
        this.configuration = configuration;
        this.outputDirectory = outputDirectory;
        this.mappersOutputPath = mappersOutputDirectory;
        this.workerManager = new WorkerManager();
        this.workerEndpoint = new WorkerEndpoint(workerManager);
        this.workerEndpoint.startServer(serverPort);
    }

    public void start() {
        while (!Thread.currentThread().isInterrupted()) {
            synchronized (workerManager) {
                while (workerManager.getCurrentTask().isEmpty()) {
                    try {
                        workerManager.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            executeTasks();
        }
    }

    private void executeTasks() {
        Optional<WorkerManager.Task> taskOpt = workerManager.getCurrentTask();
        if (taskOpt.isPresent()) {
            WorkerManager.Task task = taskOpt.get();
            try {
                if (task.getType() == WorkerManager.TaskType.MAP) {
                    mapperJob(task.getInputFiles(), task.getTaskId());
                } else if (task.getType() == WorkerManager.TaskType.REDUCE) {
                    reduceJob(task.getInputFiles(), task.getTaskId());
                }
                workerManager.completeCurrentTask();
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }

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
}
