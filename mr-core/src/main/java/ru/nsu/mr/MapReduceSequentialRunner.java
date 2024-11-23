package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.CoordinatorEndpoint;
import ru.nsu.mr.endpoints.LoggerWithMetricsCalculation;
import ru.nsu.mr.endpoints.MetricsService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class MapReduceSequentialRunner<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT>
        implements MapReduceRunner<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> {

    public MapReduceSequentialRunner() {}

    @Override
    public void run(
            MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job,
            List<Path> inputFiles,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory) {
        Logger logger = new LoggerWithMetricsCalculation();
        CoordinatorEndpoint endpoint = null;

        if (!configuration.get(ConfigurationOption.METRICS_PORT).isEmpty()) {
            try {
                endpoint =
                        new CoordinatorEndpoint(
                                configuration.get(ConfigurationOption.METRICS_PORT),
                                (MetricsService) logger,
                                (e) -> {},
                                (e) -> {});
                endpoint.startServer();
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }

        String jobId = "1";

        logger.jobReceived(jobId, jobId);
        logger.jobStart(jobId);

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
                        inputFilesToProcess, i, mappersOutputDirectory, configuration, job);
            } catch (IOException e) {
                throw new RuntimeException();
            }
            numberOfProcessedInputFiles += inputFilesToProcessCount;
        }
        for (int i = 0; i < configuration.get(ConfigurationOption.REDUCERS_COUNT); ++i) {
            List<Path> interFilesToReduce = new ArrayList<>();
            for (int k = 0; k < mappersCount; ++k) {
                interFilesToReduce.add(
                        mappersOutputDirectory.resolve("mapper-output-" + k + "-" + i + ".txt"));
            }
            try {
                MapReduceTasksRunner.executeReduceTask(
                        interFilesToReduce, i, outputDirectory, configuration, job);
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }

        logger.jobFinish(jobId);

        if (endpoint != null) {
            endpoint.stopServer();
        }
    }
}
