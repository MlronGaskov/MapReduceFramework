package ru.nsu.mr;

public class Application {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("""
                    Usage:
                    Job mode: java -jar app.jar coordinator <config.yaml> <coordinatorBaseUrl>
                    Waiting mode: java -jar app.jar coordinator <coordinatorBaseUrl>
                    Worker mode: java -jar app.jar worker <coordinatorBaseUrl> <workerBaseUrl>""");
            System.exit(1);
        }
        String mode = args[0];
        try {
            if ("coordinator".equalsIgnoreCase(mode)) {
                Coordinator coordinator;
                if (args.length == 3) {
                    String configFile = args[1];
                    String coordinatorBaseUrl = args[2];
                    coordinator = new Coordinator(coordinatorBaseUrl);
                    coordinator.setJobConfiguration(configFile);
                } else if (args.length == 2) {
                    String coordinatorBaseUrl = args[1];
                    coordinator = new Coordinator(coordinatorBaseUrl);
                } else {
                    System.err.println("""
                            Usage for coordinator:
                            Job mode: java -jar app.jar coordinator <config.yaml> <coordinatorBaseUrl>
                            Waiting mode: java -jar app.jar coordinator <coordinatorBaseUrl>""");
                    System.exit(1);
                    return;
                }
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
