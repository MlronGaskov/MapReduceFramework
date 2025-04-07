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
        if (args.length < 1) {
            System.err.println("Usage: java -jar app.jar <mode> [other args]");
            System.exit(1);
        }
        String mode = args[0];
        try {
            if ("coordinator".equalsIgnoreCase(mode)) {
                Coordinator coordinator;
                String configFile = null;
                String baseUrl;

                if (args.length == 1) {
                    baseUrl = autoAssignBaseUrl();
                } else if (args.length == 2) {
                    if (args[1].endsWith(".yaml")) {
                        configFile = args[1];
                        baseUrl = autoAssignBaseUrl();
                    } else {
                        baseUrl = args[1];
                    }
                } else {
                    configFile = args[1];
                    baseUrl = args[2];
                }

                coordinator = new Coordinator(baseUrl);
                if (configFile != null) {
                    coordinator.setJobConfiguration(configFile);
                }
                System.out.println("Coordinator started at " + baseUrl);
                coordinator.start();
            } else if ("worker".equalsIgnoreCase(mode)) {
                if (args.length < 2) {
                    System.err.println("Usage: java -jar app.jar worker <coordinatorBaseUrl> [workerBaseUrl]");
                    System.exit(1);
                }
                String coordinatorBaseUrl = args[1];
                String workerBaseUrl;
                if (args.length >= 3) {
                    workerBaseUrl = args[2];
                } else {
                    workerBaseUrl = autoAssignBaseUrl();
                }
                Worker worker = new Worker(coordinatorBaseUrl, workerBaseUrl, "./logs");
                System.out.println("Worker started at " + workerBaseUrl + " connecting to coordinator at " + coordinatorBaseUrl);
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