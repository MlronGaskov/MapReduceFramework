package ru.nsu.mr.config;

public class ConfigurationOption<T> {
    final String name;
    final T defaultValue;

    public static ConfigurationOption<Integer> JOB_ID =
            new ConfigurationOption<>("mr.job.id", 1);

    public static ConfigurationOption<String> JOB_PATH =
            new ConfigurationOption<>("mr.job.path", null);

    public static ConfigurationOption<String> JOB_STORAGE_CONNECTION_STRING =
            new ConfigurationOption<>("mr.job.storage.connection.string", null);

    public static ConfigurationOption<String> INPUTS_PATH =
            new ConfigurationOption<>("mr.inputs.path", null);

    public static ConfigurationOption<String> MAPPERS_OUTPUTS_PATH =
            new ConfigurationOption<>("mr.mappers.outputs.path", null);

    public static ConfigurationOption<String> REDUCERS_OUTPUTS_PATH =
            new ConfigurationOption<>("mr.reducers.outputs.path", null);

    public static ConfigurationOption<String> DATA_STORAGE_CONNECTION_STRING =
            new ConfigurationOption<>("mr.data.storage.connection.string", null);

    public static ConfigurationOption<Integer> MAPPERS_COUNT =
            new ConfigurationOption<>("mr.mappers.count", 1);

    public static ConfigurationOption<Integer> REDUCERS_COUNT =
            new ConfigurationOption<>("mr.reducers.count", 1);

    public static ConfigurationOption<Integer> SORTER_IN_MEMORY_RECORDS =
            new ConfigurationOption<>("mr.sorter.in.memory.records", 10000);

    public ConfigurationOption(String name, T defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }
}
