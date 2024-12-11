package ru.nsu.mr;


import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class Launcher {
    public static void launch(MapReduceJob job, Configuration config, Path mappersOutputDirectory,
                              Path reducersOutputDirectory, List<Path> inputPaths, String[] args) {
        String role = args[0];
        String serverPort; // Порт для WorkerEndpoint
        String coordinatorPort = config.get(ConfigurationOption.METRICS_PORT);

        if ("worker".equalsIgnoreCase(role)) {
            serverPort = args[1];
            // Запуск Worker
            try {
                Worker worker = new Worker(job, config, mappersOutputDirectory, reducersOutputDirectory, serverPort, coordinatorPort);
                worker.start();
            } catch (IOException e) {
                System.err.println("Failed to start Worker: " + e.getMessage());
            }
        } else if ("coordinator".equalsIgnoreCase(role)) {
            // Запуск Coordinator
            try {
                Coordinator coordinator = new Coordinator(config);
                coordinator.start(inputPaths);
            } catch (IOException | InterruptedException e) {
                System.err.println("Failed to start Coordinator: " + e.getMessage());
            }
        } else {
            System.err.println("Unknown role: " + role + ". Use 'worker' or 'coordinator'.");
        }
    }
}
