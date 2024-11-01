package ru.nsu.mr.sinks;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SortedFileSinkTest {
    private Path outputFilePath;
    private SortedFileSink<Integer, Integer> sortedFileSink;

    @BeforeEach
    public void setUp() throws IOException {
        outputFilePath = Files.createTempFile("sorted_output", ".txt");
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

    private List<Integer> readSortedNumbersFromFile(Path filePath) throws IOException {
        List<Integer> numbers = new ArrayList<>();
        List<String> lines = Files.readAllLines(filePath);
        for (String line : lines) {
            String[] parts = line.split(" ");
            numbers.add(Integer.parseInt(parts[0]));
        }
        return numbers;
    }
}
