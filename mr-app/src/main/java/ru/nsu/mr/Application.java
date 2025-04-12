package ru.nsu.mr;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Enumeration;

public class Application {

    public static String getNonLocalIpAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()){
                NetworkInterface ni = interfaces.nextElement();
                if (ni.isUp() && !ni.isLoopback()) {
                    Enumeration<InetAddress> addresses = ni.getInetAddresses();
                    while (addresses.hasMoreElements()){
                        InetAddress address = addresses.nextElement();
                        if (!address.isLoopbackAddress() && address instanceof Inet4Address) {
                            return address.getHostAddress();
                        }
                    }
                }
            }
        } catch (SocketException e) {
            e.fillInStackTrace();
        }
        return null;
    }

    public static int findFreePort(int startPort) {
        int port = startPort;
        while (true) {
            try (ServerSocket socket = new ServerSocket(port)) {
                return port;
            } catch (IOException e) {
                port++;
            }
        }
    }

    public static String autoAssignBaseUrl() {
        String ip = getNonLocalIpAddress();
        if (ip == null) {
            ip = "127.0.0.1";
        }
        int port = findFreePort(8080);
        return "http://" + ip + ":" + port;
    }

    public static void main(String[] args) {
        String logsDestination = "./logs";
        int index = 0;

        if (args.length >= 2 && args[0].equals("--logs")) {
            logsDestination = args[1];
            index = 2;
        }

        if (args.length - index < 1) {
            System.err.println("Usage: java -jar app.jar <mode> [other args]");
            System.exit(1);
        }

        String mode = args[index];
        try {
            if ("coordinator".equalsIgnoreCase(mode)) {
                Coordinator coordinator;
                String configFile = null;
                String baseUrl;

                if (args.length - index == 1) {
                    baseUrl = autoAssignBaseUrl();
                } else if (args.length - index == 2) {
                    if (args[index + 1].endsWith(".yaml")) {
                        configFile = args[index + 1];
                        baseUrl = autoAssignBaseUrl();
                    } else {
                        baseUrl = args[index + 1];
                    }
                } else {
                    configFile = args[index + 1];
                    baseUrl = args[index + 2];
                }

                coordinator = new Coordinator(baseUrl, logsDestination);
                if (configFile != null) {
                    coordinator.setJobConfiguration(configFile);
                }
                System.out.println("Coordinator started at " + baseUrl);
                coordinator.start();
            } else if ("worker".equalsIgnoreCase(mode)) {
                if (args.length - index < 2) {
                    System.err.println("Usage: java -jar app.jar worker <coordinatorBaseUrl> [workerBaseUrl]");
                    System.exit(1);
                }
                String coordinatorBaseUrl = args[index + 1];
                String workerBaseUrl;
                if (args.length - index >= 3) {
                    workerBaseUrl = args[index + 2];
                } else {
                    workerBaseUrl = autoAssignBaseUrl();
                }

                Worker worker = new Worker(coordinatorBaseUrl, workerBaseUrl, logsDestination);
                System.out.println("Worker started at " + workerBaseUrl + " connecting to coordinator at " + coordinatorBaseUrl);
                worker.start();
            } else {
                System.err.println("Unknown mode: " + mode);
                System.exit(1);
            }
        } catch (Exception e) {
            e.fillInStackTrace();
            System.exit(1);
        }
    }
}
