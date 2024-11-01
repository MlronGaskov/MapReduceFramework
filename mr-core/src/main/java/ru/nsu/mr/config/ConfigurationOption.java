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

    public ConfigurationOption(String name, T defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }
}
