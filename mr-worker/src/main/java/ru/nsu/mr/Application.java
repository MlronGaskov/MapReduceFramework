package ru.nsu.mr;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar Application.jar <path-to-config> <additional-args>");
            System.exit(1);
        }
        String[] additionalArgs;
        if (args.length > 1) {
            additionalArgs = new String[args.length];
            additionalArgs[0] = "worker";
            System.arraycopy(args, 1, additionalArgs, 1, args.length - 1);
        } else {
            additionalArgs = new String[]{"worker"};
        }
        ConfigurationLoader loader = new ConfigurationLoader(args[0]);
        JarFileParser jarFileParser = new JarFileParser(loader.getJarPath());
        MapReduceJob<?, ?, ?, ?> job = jarFileParser.loadUsersSubClass(MapReduceJob.class);
        Launcher.launch(job, loader.getConfig(), loader.getInputFilesDirectory(), loader.getMappersOutputPath(),
                loader.getReducersOutputPath(), loader.getStorageConnectionString(), additionalArgs);
    }
}