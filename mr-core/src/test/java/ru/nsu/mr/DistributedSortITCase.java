package ru.nsu.mr;

import static org.junit.jupiter.api.Assertions.*;
import static ru.nsu.mr.PredefinedFunctions.*;
import static ru.nsu.mr.config.ConfigurationOption.MAPPERS_COUNT;
import static ru.nsu.mr.config.ConfigurationOption.REDUCERS_COUNT;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.nsu.mr.config.Configuration;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import java.util.concurrent.ThreadLocalRandom;

class DistributedSortITCase {
    private Path reducersOutputPath;
    private Path mappersOutputPath;

    @BeforeEach
    public void setUp() throws IOException {
        reducersOutputPath = Files.createTempDirectory("outputs");
        mappersOutputPath = Files.createTempDirectory("mappers_outputs");
    }

    @AfterEach
    public void tearDown() throws IOException {
        deleteDirectory(reducersOutputPath);
        deleteDirectory(mappersOutputPath);
    }

    static class DistributedSortMapper implements Mapper<String, String, String, Integer> {
        @Override
        public void map(
                Iterator<Pair<String, String>> input, OutputContext<String, Integer> output) {
            while (input.hasNext()) {
                Pair<String, String> record = input.next();
                String key = extractKey(record.value());
                String value = extractValue(record.value());
                output.put(key, Integer.valueOf(value));
            }
        }

        private String extractKey(String record) {
            // Здесь предполагается, что ключ - это первая часть записи до первого пробела.
            return record.split(" ")[0];
        }

        private String extractValue(String record) {
            return record.split(" ")[1];
        }
    }

    static class DistributedSortReducer extends AfterMapReducer<String, Integer, String, Integer> {
        @Override
        public void reduce(
                String key, Iterator<Integer> values, OutputContext<String, Integer> output) {
            while (values.hasNext()) {
                output.put(key, values.next());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("sortParameters")
    public void testDistributedSort(SortConfig testConfig) throws IOException {
        int fileCount = testConfig.fileCount;
        int recordsPerFile = testConfig.recordsPerFile;
        int mappersCount = testConfig.mappersCount;
        int reducersCount = testConfig.reducersCount;

        List<Path> inputFiles = generateInputFiles(fileCount, recordsPerFile);

        MapReduceJob<String, Integer, String, Integer> job =
                new MapReduceJob<>(
                        new DistributedSortMapper(),
                        new DistributedSortReducer(),
                        STRING_SERIALIZER,
                        INTEGER_SERIALIZER,
                        STRING_DESERIALIZER,
                        INTEGER_DESERIALIZER,
                        STRING_SERIALIZER,
                        INTEGER_SERIALIZER,
                        STRING_DESERIALIZER,
                        INTEGER_DESERIALIZER,
                        STRING_KEY_COMPARATOR,
                        STRING_KEY_COMPARATOR,
                        STRING_KEY_HASH);

        Configuration config =
                new Configuration()
                        .set(MAPPERS_COUNT, mappersCount)
                        .set(REDUCERS_COUNT, reducersCount);

        MapReduceRunner mr = new MapReduceSequentialRunner();

        mr.run(job, inputFiles, config, mappersOutputPath, reducersOutputPath);

        // Проверка ожидаемого результата
        checkResult(testConfig, reducersOutputPath);
    }

    private void checkResult(SortConfig config, Path outputPath) throws IOException {
        List<String> sortedResults = new ArrayList<>();
        for (int i = 0; i < config.reducersCount; ++i) {
            readSortedResult(outputPath.toString() + "/output-" + i + ".txt", sortedResults);
        }

        List<String> keys = new ArrayList<>(sortedResults);
        Collections.sort(keys);
        assertEquals(keys, sortedResults);
    }

    public static class SortConfig {
        int fileCount;
        int recordsPerFile;
        int mappersCount;
        int reducersCount;

        SortConfig(int fileCount, int recordsPerFile, int reducersCount, int mappersCount) {
            this.fileCount = fileCount;
            this.recordsPerFile = recordsPerFile;
            this.reducersCount = reducersCount;
            this.mappersCount = mappersCount;
        }

        @Override
        public String toString() {
            return "M = "
                    + mappersCount
                    + ", R = "
                    + reducersCount
                    + ", files = "
                    + fileCount
                    + ", record per file = "
                    + recordsPerFile;
        }
    }

    static Stream<SortConfig> sortParameters() {
        return Stream.of(new SortConfig(5, 10, 1, 2),
                new SortConfig(5, 4, 1, 5)
        );
    }

    private void deleteDirectory(Path path) throws IOException {
        try (Stream<Path> pathStream = Files.walk(path)) {
            pathStream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    private List<Path> generateInputFiles(int fileCount, int recordsPerFile) throws IOException {
        List<Path> inputFiles = new ArrayList<>();
        for (int i = 1; i <= fileCount; i++) {
            Path tempFile = Files.createTempFile("InputFile" + i, ".txt");
            try (BufferedWriter writer = Files.newBufferedWriter(tempFile)) {
                for (int j = 0; j < recordsPerFile; j++) {
                    writer.write(
                            "key:"
                                    + (ThreadLocalRandom.current().nextInt(0, recordsPerFile))
                                    + " "
                                    + j);
                    writer.newLine();
                }
            }
            inputFiles.add(tempFile);
        }
        return inputFiles;
    }

    public static void readSortedResult(String filename, List<String> result) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                result.add(parts[0]);
            }
        }
    }
}
