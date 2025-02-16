package ru.nsu.mr;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java -jar Application.jar <path-to-config> <additional-args>");
            System.exit(1);
        }
        String[] additionalArgs = Arrays.copyOfRange(args, 1, args.length);
        ConfigurationLoader loader = new ConfigurationLoader(args[0]);
        JarFileParser jarFileParser = new JarFileParser(loader.getJarPath());
        MapReduceJob<?, ?, ?, ?> job = jarFileParser.loadUsersSubClass(MapReduceJob.class);
        Launcher.launch(job, loader.getConfig(), loader.getInputFilesDirectory(), loader.getMappersOutputPath(),
                loader.getReducersOutputPath(), loader.getStorageConnectionString(), additionalArgs);
    }
}