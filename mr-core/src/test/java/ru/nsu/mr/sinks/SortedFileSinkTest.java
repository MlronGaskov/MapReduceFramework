package ru.nsu.mr.sinks;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class SortedFileSinkTest {
    private Path outputFilePath;
    private SortedFileSink<Integer, Integer> sortedFileSink;

    @BeforeEach
    public void setUp() throws IOException {
        outputFilePath = Files.createTempFile("sorted_output", ".zip");
        sortedFileSink =
                new SortedFileSink<>(
                        Object::toString,
                        Object::toString,
                        Integer::parseInt,
                        Integer::parseInt,
                        outputFilePath,
                        1000,
                        Integer::compareTo);
    }

    @AfterEach
    public void tearDown() throws IOException {
        Files.deleteIfExists(outputFilePath);
    }

    @Test
    public void testSortedKeyValueFileSink() throws IOException {
        List<Integer> mixedNumbers = generateMixedNumbers(1, 300000);
        for (Integer number : mixedNumbers) {
            sortedFileSink.put(number, number);
        }
        sortedFileSink.close();
        mixedNumbers.sort(Integer::compareTo);
        List<Integer> sortedNumbers = readSortedNumbersFromFile(outputFilePath);
        for (int i = 0; i < sortedNumbers.size(); i++) {
            assertEquals(mixedNumbers.get(i), sortedNumbers.get(i));
        }
    }

    private List<Integer> generateMixedNumbers(int start, int end) {
        List<Integer> numbers = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            numbers.add(i);
        }
        Collections.shuffle(numbers);
        return numbers;
    }

    private List<Integer> readSortedNumbersFromFile(Path zipFilePath) throws IOException {
        List<Integer> numbers = new ArrayList<>();

        try (ZipInputStream zipInputStream = new ZipInputStream(Files.newInputStream(zipFilePath))) {
            ZipEntry entry = zipInputStream.getNextEntry();
            if (entry == null) {
                throw new IOException("ZIP archive is empty: " + zipFilePath);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(zipInputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(" ");
                    numbers.add(Integer.parseInt(parts[0]));
                }
            }
        }

        return numbers;
    }

}
