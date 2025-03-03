package ru.nsu.mr;

public class Application {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -jar app.jar [coordinator|worker] ...");
            System.exit(1);
        }
        String mode = args[0];
        try {
            if ("coordinator".equalsIgnoreCase(mode)) {
                if (args.length != 3) {
                    System.err.println("Usage: java -jar app.jar coordinator <config.yaml> <coordinatorBaseUrl>");
                    System.exit(1);
                }
                String configFile = args[1];
                String coordinatorBaseUrl = args[2];
                Coordinator coordinator = new Coordinator(coordinatorBaseUrl);
                coordinator.setJobConfiguration(configFile);
                coordinator.start();
            } else if ("worker".equalsIgnoreCase(mode)) {
                if (args.length != 3) {
                    System.err.println("Usage: java -jar app.jar worker <coordinatorBaseUrl> <workerBaseUrl>");
                    System.exit(1);
                }
                String coordinatorBaseUrl = args[1];
                String workerBaseUrl = args[2];
                Worker worker = new Worker(coordinatorBaseUrl, workerBaseUrl, "./logs");
                worker.start();
            } else {
                System.err.println("Unknown mode: " + mode);
                System.exit(1);
            }
        } catch (Exception e) {
            System.exit(1);
        }
    }
}
