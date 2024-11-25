package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ParallelRunner implements MapReduceRunner {

    public ParallelRunner() {}

    @Override
    public void run(
            MapReduceJob<?, ?, ?, ?> job,
            List<Path> inputFiles,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory) {

        Thread coordinatorThread =
                new Thread(
                        () -> {
                            try {
                                Coordinator coordinator = new Coordinator(configuration);
                                coordinator.start(inputFiles);
                            } catch (IOException | InterruptedException e) {
                                System.err.println("Error starting coordinator: " + e.getMessage());
                                throw new RuntimeException(e);
                            }
                        });
        coordinatorThread.start();

        List<Thread> workersThreads = new ArrayList<>();
        int workerCount = configuration.get(ConfigurationOption.WORKERS_COUNT);
        for (int i = 0; i < workerCount; i++) {
            String workerPort = String.valueOf(8081 + i);
            int finalI = i;
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    Worker worker =
                                            new Worker(
                                                    job,
                                                    configuration,
                                                    mappersOutputDirectory,
                                                    outputDirectory,
                                                    workerPort,
                                                    configuration.get(
                                                            ConfigurationOption.METRICS_PORT));
                                    worker.start();
                                } catch (IOException e) {
                                    System.err.println(
                                            "Error starting worker "
                                                    + finalI
                                                    + ": "
                                                    + e.getMessage());
                                }
                            });
            thread.start();
            workersThreads.add(thread);
        }

        try {
            coordinatorThread.join();
            System.out.println("Coordinator has finished its work.");
            for (int i = 0; i < workerCount; ++i) {
                workersThreads.get(i).interrupt();
                workersThreads.get(i).join();
            }
        } catch (InterruptedException e) {
            System.err.println(
                    "Interrupted while waiting for coordinator to finish: " + e.getMessage());
        }
    }
}
