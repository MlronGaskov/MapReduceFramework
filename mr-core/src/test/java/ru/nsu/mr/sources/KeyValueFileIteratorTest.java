package ru.nsu.mr.sources;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ru.nsu.mr.Deserializer;
import ru.nsu.mr.Pair;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class KeyValueFileIteratorTest {

    private Path tempFile;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("testKeyValueFile", ".txt");
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile)) {
            writer.write("key1 value1");
            writer.newLine();
            writer.write("key2 value2");
            writer.newLine();
            writer.write("key3 value3");
            writer.newLine();
        }
    }

    @Test
    public void testKeyValueFileIterator() throws IOException {
        Deserializer<String> stringDeserializer = str -> str;
        Deserializer<String> stringValueDeserializer = str -> str;
        KeyValueFileIterator<String, String> iterator =
                new KeyValueFileIterator<>(tempFile, stringDeserializer, stringValueDeserializer);
        int count = 0;
        while (iterator.hasNext()) {
            Pair<String, String> pair = iterator.next();
            count++;
            if (count == 1) {
                assertEquals("key1", pair.key());
                assertEquals("value1", pair.value());
            } else if (count == 2) {
                assertEquals("key2", pair.key());
                assertEquals("value2", pair.value());
            } else if (count == 3) {
                assertEquals("key3", pair.key());
                assertEquals("value3", pair.value());
            }
        }
        assertEquals(3, count);
        iterator.close();
    }

    @AfterEach
    public void tearDown() throws IOException {
        Files.deleteIfExists(tempFile);
    }
}
