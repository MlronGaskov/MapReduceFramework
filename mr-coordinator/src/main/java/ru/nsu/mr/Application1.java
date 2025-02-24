package ru.nsu.mr;

import java.util.Arrays;

public class Application1 {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar Application.jar <path-to-config> <additional-args>");
            System.exit(1);
        }
        String[] additionalArgs = {"coordinator"};
        ConfigurationLoader loader = new ConfigurationLoader(args[0]);
        JarFileParser jarFileParser = new JarFileParser(loader.getJarPath());
        MapReduceJob<?, ?, ?, ?> job = jarFileParser.loadUsersSubClass(MapReduceJob.class);
        CoordinatorLauncher.launch(job, loader.getConfig(), loader.getInputFilesDirectory(), loader.getMappersOutputPath(),
                loader.getReducersOutputPath(), loader.getStorageConnectionString(), additionalArgs);
    }
}