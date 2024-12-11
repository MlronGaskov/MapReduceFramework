package ru.nsu.mr;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConfigurationLoader {
    private Configuration config = new Configuration();
    private Path mappersOutputPath;
    private Path reducersOutputPath;
    private Path jarPath;
    private List<Path> inputFiles = new ArrayList<>();

    public ConfigurationLoader(String filePath) throws IOException {
        // Создаем экземпляр LoaderOptions
        LoaderOptions options = new LoaderOptions();

        // Создаем экземпляр Yaml с LoaderOptions
        Yaml yaml = new Yaml(options);// Создаем экземпляр Yaml с конструктором для Map
        try (FileInputStream fileInputStream = new FileInputStream(new File(filePath))) {
            // Считываем данные из YAML файла и сохраняем в Map
            Map<String, Object> yamlValues = yaml.load(fileInputStream);

            for (Map.Entry<String, Object> entry : yamlValues.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                switch (key) {
                    case "mappersCount":
                        config.set(ConfigurationOption.MAPPERS_COUNT, (Integer) value);
                        break;
                    case "reducersCount":
                        config.set(ConfigurationOption.REDUCERS_COUNT, (Integer) value);
                        break;
                    case "memoryRecords":
                        config.set(ConfigurationOption.SORTER_IN_MEMORY_RECORDS, (Integer) value);
                        break;
                    case "metricsPort":
                        config.set(ConfigurationOption.METRICS_PORT, (String) value);
                        break;
                    case "workersCount":
                        config.set(ConfigurationOption.WORKERS_COUNT, (Integer) value);
                        break;
                    case "mapperOutputDirectory":
                        this.mappersOutputPath = Path.of((String) value);
                        break;
                    case "outputDirectory":
                        this.reducersOutputPath = Path.of((String) value);
                        break;
                    case "jarPath":
                        this.jarPath = Path.of((String) value);
                        break;
                    case "inputDirectory":
                        Path inputPath = Path.of((String) value);
                        if (Files.isDirectory(inputPath)) {
                            Files.list(inputPath).forEach(inputFiles::add);
                        } else if (Files.isRegularFile(inputPath)) {
                            inputFiles.add(inputPath);
                        }
                        break;
                }
            }
        }
    }

    public Configuration getConfig() {
        return config;
    }

    public Path getMappersOutputPath() {
        return mappersOutputPath;
    }

    public Path getReducersOutputPath() {
        return reducersOutputPath;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public List<Path> getInputFiles() {
        return inputFiles;
    }
}
