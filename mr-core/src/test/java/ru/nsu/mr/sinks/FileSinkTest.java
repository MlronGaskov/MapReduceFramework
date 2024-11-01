package ru.nsu.mr.sinks;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class FileSinkTest {
    private Path tempFile;
    private FileSink<String, String> fileSink;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("test", ".txt");
        fileSink = new FileSink<>(input -> input, input -> input, tempFile);
    }

    @AfterEach
    public void tearDown() throws IOException {
        fileSink.close();
        Files.deleteIfExists(tempFile);
    }

    @Test
    public void testPutAndFileContent() throws IOException {
        fileSink.put("key1", "value1");
        fileSink.put("key2", "value2");
        fileSink.put("key3", "value3");
        fileSink.close();
        List<String> lines = Files.readAllLines(tempFile);
        assertEquals(3, lines.size());
        assertEquals("key1 value1", lines.get(0));
        assertEquals("key2 value2", lines.get(1));
        assertEquals("key3 value3", lines.get(2));
    }

    @Test
    public void testCloseWithoutPut() throws IOException {
        fileSink.close();
        List<String> lines = Files.readAllLines(tempFile);
        assertTrue(lines.isEmpty());
    }

    @Test
    public void testFileCleanUp() {
        assertTrue(Files.exists(tempFile), "Temporary file should exist before deletion");
    }
}
