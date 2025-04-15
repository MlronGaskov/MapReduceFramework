package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class MapReduceSequentialRunner implements MapReduceRunner {

    public MapReduceSequentialRunner() {}

    @Override
    public void run(
            MapReduceJob<?, ?, ?, ?> job,
            List<Path> inputFiles,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory) {

        Logger LOGGER = LogManager.getLogger();

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
                MapReduceTasksRunner.executeMapperTask(
                        inputFilesToProcess, i, mappersOutputDirectory, configuration, job, LOGGER);
            } catch (IOException e) {
                throw new RuntimeException();
            }
            numberOfProcessedInputFiles += inputFilesToProcessCount;
        }

        for (int i = 0; i < configuration.get(ConfigurationOption.REDUCERS_COUNT); ++i) {
            List<Path> interFilesToReduce = new ArrayList<>();
            for (int k = 0; k < mappersCount; ++k) {
                interFilesToReduce.add(
                        mappersOutputDirectory.resolve("mapper-output-" + k + "-" + i + ".zip"));
            }
            try {
                MapReduceTasksRunner.executeReduceTask(
                        interFilesToReduce, i, outputDirectory, configuration, job, LOGGER);
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }
}
