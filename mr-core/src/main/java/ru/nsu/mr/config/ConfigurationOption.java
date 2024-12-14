package ru.nsu.mr.config;

public class ConfigurationOption<T> {
    final String name;
    final T defaultValue;

    public static ConfigurationOption<Integer> MAPPERS_COUNT =
            new ConfigurationOption<>("runtime.mapper.count", 1);

    public static ConfigurationOption<Integer> REDUCERS_COUNT =
            new ConfigurationOption<>("runtime.reducer.count", 1);

    public static ConfigurationOption<Integer> SORTER_IN_MEMORY_RECORDS =
            new ConfigurationOption<>("sorter.memory.n-records", 10000);

    public static ConfigurationOption<String> METRICS_PORT =
            new ConfigurationOption<>("endpoints.metrics.port", "");

    public static ConfigurationOption<Integer> WORKERS_COUNT =
            new ConfigurationOption<>("runtime.worker.count", 1);

    public static ConfigurationOption<String> LOGS_PATH =
            new ConfigurationOption<>("logs.path", "");

    public ConfigurationOption(String name, T defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }
}
