package ru.nsu.mr.sinks;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class PartitionedFileSinkTest {
    private int sinksCount = 2;
    private List<Path> tempFiles;
    private List<FileSystemSink<Integer, String>> fileSinks;
    private PartitionedFileSink<Integer, String> partitionedFileSink;

    @BeforeEach
    public void setUp() throws IOException {
        tempFiles = new ArrayList<>();
        fileSinks = new ArrayList<>();
        for (int i = 0; i < sinksCount; ++i) {
            tempFiles.add(Files.createTempFile("test" + i, ".txt"));
            fileSinks.add(new FileSink<>(Object::toString, input -> input, tempFiles.get(i)));
        }
        partitionedFileSink = new PartitionedFileSink<>(fileSinks, input -> input);
    }

    @AfterEach
    public void tearDown() throws IOException {
        for (Path tempFile : tempFiles) {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    public void testPutAndFileContent() throws IOException {
        partitionedFileSink.put(5, "value1");
        partitionedFileSink.put(1, "value2");
        partitionedFileSink.put(2, "value3");
        partitionedFileSink.put(4, "value4");
        partitionedFileSink.put(5, "value5");
        partitionedFileSink.put(7, "value6");
        partitionedFileSink.put(10, "value7");
        partitionedFileSink.put(1, "value8");
        partitionedFileSink.put(0, "value9");
        partitionedFileSink.close();
        List<String> lines = Files.readAllLines(tempFiles.getFirst());
        assertEquals(4, lines.size());
        assertEquals("2 value3", lines.get(0));
        assertEquals("4 value4", lines.get(1));
        assertEquals("10 value7", lines.get(2));
        assertEquals("0 value9", lines.get(3));

        lines = Files.readAllLines(tempFiles.get(1));
        assertEquals(5, lines.size());
        assertEquals("5 value1", lines.get(0));
        assertEquals("1 value2", lines.get(1));
        assertEquals("5 value5", lines.get(2));
        assertEquals("7 value6", lines.get(3));
        assertEquals("1 value8", lines.get(4));
    }
}
