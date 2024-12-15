package ru.nsu.mr;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ConfigurationLoader {
    private Configuration configuration = new Configuration();
    private Path mapperOutputDirectory;
    private Path outputDirectory;
    private Path jarPath;
    private List<Path> inputFiles = new ArrayList<>();

    private static class Config {
        public String jarPath;
        public String inputDirectory;
        public String mapperOutputDirectory;
        public String outputDirectory;
        public String metricsPort;
        public String logsPath;
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
        mapperOutputDirectory = Path.of(config.mapperOutputDirectory);
        outputDirectory = Path.of(config.outputDirectory);
        configuration.set(ConfigurationOption.METRICS_PORT, config.metricsPort)
                .set(ConfigurationOption.MAPPERS_COUNT, config.mappersCount)
                .set(ConfigurationOption.REDUCERS_COUNT, config.reducersCount)
                .set(ConfigurationOption.LOGS_PATH, config.logsPath);
        if (config.sorterInMemoryRecords != null) {
            configuration.set(ConfigurationOption.SORTER_IN_MEMORY_RECORDS, config.sorterInMemoryRecords);
        }
        Path inputPath = Path.of(config.inputDirectory);
        if (Files.isDirectory(inputPath)) {
            Files.list(inputPath).forEach(inputFiles::add);
        } else if (Files.isRegularFile(inputPath)) {
            inputFiles.add(inputPath);
        }

    }

    public Configuration getConfig() {
        return configuration;
    }

    public Path getMappersOutputPath() {
        return mapperOutputDirectory;
    }

    public Path getReducersOutputPath() {
        return outputDirectory;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public List<Path> getInputFiles() {
        return inputFiles;
    }
}
