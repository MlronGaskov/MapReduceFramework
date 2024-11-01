package ru.nsu.mr.config;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ConfigurationTest {

    @Test
    void testSetAndGet() {
        Configuration config = new Configuration();
        ConfigurationOption<Integer> mappersCount = ConfigurationOption.MAPPERS_COUNT;
        config.set(mappersCount, 5);
        Integer value = config.get(mappersCount);
        assertEquals(5, value);
    }

    @Test
    void testGetDefaultValueWhenNotSet() {
        Configuration config = new Configuration();
        ConfigurationOption<Integer> mappersCount = ConfigurationOption.MAPPERS_COUNT;
        Integer value = config.get(mappersCount);
        assertEquals(1, value);
    }

    @Test
    void testIsSetReturnsTrue() {
        Configuration config = new Configuration();
        ConfigurationOption<Integer> mappersCount = ConfigurationOption.MAPPERS_COUNT;
        config.set(mappersCount, 5);
        assertTrue(config.isSet(mappersCount));
    }

    @Test
    void testIsSetReturnsFalse() {
        Configuration config = new Configuration();
        ConfigurationOption<Integer> mappersCount = ConfigurationOption.MAPPERS_COUNT;
        assertFalse(config.isSet(mappersCount));
    }
}
