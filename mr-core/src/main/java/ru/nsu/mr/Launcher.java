package ru.nsu.mr;


import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;

import java.io.IOException;

public class Launcher {
    public static void launch(MapReduceJob<?, ?, ?, ?> job, Configuration config, String inputDirectory,
                              String mappersOutputDirectory, String reducersOutputDirectory,
                              String storageConnectionString, String[] args) {
        String role = args[0];
        String serverPort;
        String coordinatorPort = config.get(ConfigurationOption.METRICS_PORT);

        if ("worker".equalsIgnoreCase(role)) {
            serverPort = args[1];
            try {
                Worker worker = new Worker(job, config, serverPort, coordinatorPort);
                worker.start();
            } catch (IOException e) {
                System.err.println("Failed to start Worker: " + e.getMessage());
            }
        } else if ("coordinator".equalsIgnoreCase(role)) {
            try {
                Coordinator coordinator = new Coordinator(config, mappersOutputDirectory, reducersOutputDirectory);
                coordinator.start(inputDirectory, storageConnectionString);
            } catch (IOException | InterruptedException e) {
                System.err.println("Failed to start Coordinator: " + e.getMessage());
            }
        } else {
            System.err.println("Unknown role: " + role + ". Use 'worker' or 'coordinator'.");
        }
    }
}
