package ru.nsu.mr;


import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;

import java.io.IOException;

public class CoordinatorLauncher {
    public static void launch(MapReduceJob<?, ?, ?, ?> job, Configuration config, String inputDirectory,
                              String mappersOutputDirectory, String reducersOutputDirectory,
                              String storageConnectionString, String[] args) {
        String role = args[0];
        String serverPort;
        String coordinatorPort = config.get(ConfigurationOption.METRICS_PORT);

        if ("coordinator".equalsIgnoreCase(role)) {
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
