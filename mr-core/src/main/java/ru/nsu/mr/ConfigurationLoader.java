package ru.nsu.mr;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class ConfigurationLoader {
    private final Configuration configuration = new Configuration();
    private final String mapperOutputDirectory;
    private final String outputDirectory;
    private final Path jarPath;
    private final String inputDirectory;
    private final String storageConnectionString;

    private static class Config {
        public String jarPath;
        public String inputDirectory;
        public String mapperOutputDirectory;
        public String outputDirectory;
        public String metricsPort;
        public String logsPath;
        public String storageConnectionString;
        public Integer mappersCount;
        public Integer reducersCount;
        public Integer sorterInMemoryRecords;
    }

    public ConfigurationLoader(String filePath) throws IOException {

        // Создаем экземпляр LoaderOptions
        LoaderOptions options = new LoaderOptions();

        // Создаем экземпляр Yaml с LoaderOptions
        Yaml yaml = new Yaml(options);


        Config config;
        try (InputStream inputStream = Files.newInputStream(Path.of(filePath))) {
            config = yaml.loadAs(inputStream, Config.class);
        }

        jarPath = Path.of(config.jarPath);
        mapperOutputDirectory = config.mapperOutputDirectory;
        this.storageConnectionString = config.storageConnectionString;
        this.inputDirectory = config.inputDirectory;
        outputDirectory = config.outputDirectory;
        configuration.set(ConfigurationOption.METRICS_PORT, config.metricsPort)
                .set(ConfigurationOption.MAPPERS_COUNT, config.mappersCount)
                .set(ConfigurationOption.REDUCERS_COUNT, config.reducersCount)
                .set(ConfigurationOption.LOGS_PATH, config.logsPath);
        if (config.sorterInMemoryRecords != null) {
            configuration.set(ConfigurationOption.SORTER_IN_MEMORY_RECORDS, config.sorterInMemoryRecords);
        }
    }

    public Configuration getConfig() {
        return configuration;
    }

    public String getMappersOutputPath() {
        return mapperOutputDirectory;
    }

    public String getReducersOutputPath() {
        return outputDirectory;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public String getInputFilesDirectory() {
        return inputDirectory;
    }

    public String getStorageConnectionString() {
        return storageConnectionString;
    }
}
