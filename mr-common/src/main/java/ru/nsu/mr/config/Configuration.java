package ru.nsu.mr.config;

import java.util.HashMap;
import java.util.Map;

public class Configuration {
    final Map<String, Object> values = new HashMap<>();

    public <T> Configuration set(ConfigurationOption<T> option, T value) {
        values.put(option.name, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(ConfigurationOption<T> option) {
        if (values.containsKey(option.name)) {
            return (T) values.get(option.name);
        }
        return option.defaultValue;
    }
}
