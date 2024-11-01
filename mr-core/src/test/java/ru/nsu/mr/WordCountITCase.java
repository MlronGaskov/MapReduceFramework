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

class WordCountITCase {
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

    static class WordCountMapper implements Mapper<String, String, String, Integer> {
        @Override
        public void map(
                Iterator<Pair<String, String>> input, OutputContext<String, Integer> output) {
            while (input.hasNext()) {
                Pair<String, String> split = input.next();
                for (String word : split.value().split("[\\s.,]+")) {
                    output.put(word.trim().toLowerCase(), 1);
                }
            }
        }
    }

    static class WordCountReducer implements Reducer<String, Integer, String, Integer> {
        @Override
        public void reduce(
                String key, Iterator<Integer> values, OutputContext<String, Integer> output) {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next();
            }
            output.put(key, sum);
        }
    }

    @ParameterizedTest
    @MethodSource("wordCounterParameters")
    public void testWordCounter(WordCounterConfig testConfig) throws IOException {
        int inputFilesCount = testConfig.inputFilesCount;
        int eachWordPerFileCount = testConfig.eachWordPerFileCount;
        int mappersCount = testConfig.mappersCount;
        int reducersCount = testConfig.reducersCount;

        List<Path> inputFiles =
                generatesInputFiles(inputFilesCount, testConfig.WORDS, eachWordPerFileCount);

        MapReduceJob<String, Integer, String, Integer> job =
                new MapReduceJob<>(
                        new WordCountMapper(),
                        new WordCountReducer(),
                        STRING_SERIALIZER,
                        INTEGER_SERIALIZER,
                        STRING_DESERIALIZER,
                        INTEGER_DESERIALIZER,
                        STRING_SERIALIZER,
                        INTEGER_SERIALIZER,
                        STRING_KEY_COMPARATOR,
                        STRING_KEY_HASH);

        Configuration config =
                new Configuration()
                        .set(MAPPERS_COUNT, mappersCount)
                        .set(REDUCERS_COUNT, reducersCount);

        MapReduceRunner<String, Integer, String, Integer> mr = new MapReduceSequentialRunner<>();

        mr.run(job, inputFiles, config, mappersOutputPath, reducersOutputPath);

        HashMap<String, Integer> mappersResult = new HashMap<>();
        for (int i = 0; i < mappersCount; ++i) {
            for (int j = 0; j < reducersCount; ++j) {
                readResult(
                        mappersOutputPath.toString() + "/mapper-output-" + i + "-" + j + ".txt",
                        mappersResult);
            }
        }
        for (String word : testConfig.WORDS) {
            int wordCntResult = mappersResult.get(word);
            assertEquals(inputFilesCount * eachWordPerFileCount, wordCntResult);
        }

        HashMap<String, Integer> reducesResult = new HashMap<>();
        for (int i = 0; i < reducersCount; ++i) {
            readResult(reducersOutputPath.toString() + "/output-" + i + ".txt", reducesResult);
        }
        for (String word : testConfig.WORDS) {
            int wordCntResult = reducesResult.get(word);
            assertEquals(inputFilesCount * eachWordPerFileCount, wordCntResult);
        }
    }

    public static void readResult(String filename, Map<String, Integer> result) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(" ");
            result.put(parts[0], result.getOrDefault(parts[0], 0) + Integer.parseInt(parts[1]));
        }
    }

    public static class WordCounterConfig {
        final String[] WORDS = {
            "apple", "banana", "orange", "grape", "pear", "kiwi", "melon", "peach"
        };
        int eachWordPerFileCount;
        int inputFilesCount;
        int mappersCount;

        int reducersCount;

        WordCounterConfig(
                int eachWordCount, int inputFilesCount, int mappersCount, int reducersCount) {
            this.eachWordPerFileCount = eachWordCount;
            this.inputFilesCount = inputFilesCount;
            this.mappersCount = mappersCount;
            this.reducersCount = reducersCount;
        }

        @Override
        public String toString() {
            return "M = "
                    + mappersCount
                    + ", R = "
                    + reducersCount
                    + ", files = "
                    + inputFilesCount
                    + ", each word per file = "
                    + eachWordPerFileCount;
        }
    }

    static Stream<WordCounterConfig> wordCounterParameters() {
        return Stream.of(new WordCounterConfig(10, 10, 3, 4), new WordCounterConfig(5, 50, 2, 3));
    }

    private void deleteDirectory(Path path) throws IOException {
        try (Stream<Path> pathStream = Files.walk(path)) {
            pathStream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    public static ArrayList<String> generateWords(String[] words, int count) {
        ArrayList<String> result = new ArrayList<>();
        for (String word : words) {
            for (int i = 0; i < count; ++i) {
                result.add(word);
            }
        }
        Collections.shuffle(result);
        return result;
    }

    private static List<Path> generatesInputFiles(
            int inputFilesCount, String[] words, int eachWordCount) throws IOException {
        List<Path> inputFiles = new ArrayList<>();
        for (int i = 1; i <= inputFilesCount; ++i) {
            Path tempFile = Files.createTempFile("TestFile" + i, ".txt");
            BufferedWriter writer = Files.newBufferedWriter(tempFile);
            writer.write(String.join(" ", generateWords(words, eachWordCount)));
            writer.close();
            inputFiles.add(tempFile);
        }
        return inputFiles;
    }
}
