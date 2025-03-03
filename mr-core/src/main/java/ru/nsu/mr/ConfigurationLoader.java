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

    private static class Config {
        public Integer jobId;
        public String jobPath;
        public String jobStorageConnectionString;
        public String inputsPath;
        public String mappersOutputsPath;
        public String reducersOutputsPath;
        public String dataStorageConnectionString;
        public Integer mappersCount;
        public Integer reducersCount;
        public Integer sorterInMemoryRecords;
    }

    public ConfigurationLoader(String filePath) throws IOException {
        LoaderOptions options = new LoaderOptions();
        Yaml yaml = new Yaml(options);

        Config config;
        try (InputStream inputStream = Files.newInputStream(Path.of(filePath))) {
            config = yaml.loadAs(inputStream, Config.class);
        }

        configuration.set(ConfigurationOption.JOB_ID, config.jobId)
                .set(ConfigurationOption.JOB_PATH, config.jobPath)
                .set(ConfigurationOption.JOB_STORAGE_CONNECTION_STRING, config.jobStorageConnectionString)
                .set(ConfigurationOption.INPUTS_PATH, config.inputsPath)
                .set(ConfigurationOption.MAPPERS_OUTPUTS_PATH, config.mappersOutputsPath)
                .set(ConfigurationOption.REDUCERS_OUTPUTS_PATH, config.reducersOutputsPath)
                .set(ConfigurationOption.DATA_STORAGE_CONNECTION_STRING, config.dataStorageConnectionString)
                .set(ConfigurationOption.MAPPERS_COUNT, config.mappersCount)
                .set(ConfigurationOption.REDUCERS_COUNT, config.reducersCount)
                .set(ConfigurationOption.SORTER_IN_MEMORY_RECORDS, config.sorterInMemoryRecords);
    }

    public Configuration getConfig() {
        return configuration;
    }
}
