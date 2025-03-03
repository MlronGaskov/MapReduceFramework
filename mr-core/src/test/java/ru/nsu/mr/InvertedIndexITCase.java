package ru.nsu.mr;

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

class InvertedIndexITCase {
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

    static class InvertedIndexMapper implements Mapper<String, String, String, String> {
        @Override
        public void map(
                Iterator<Pair<String, String>> input, OutputContext<String, String> output) {
            while (input.hasNext()) {
                Pair<String, String> document = input.next(); // document ID, document text
                String docId = document.key();
                String content = document.value();

                for (String word : content.split("[\\s.,]+")) {
                    output.put(word.trim().toLowerCase(), docId);
                }
            }
        }
    }

    static class InvertedIndexReducer implements Reducer<String, String, String, List<String>> {
        @Override
        public void reduce(
                String key, Iterator<String> values, OutputContext<String, List<String>> output) {
            Set<String> documentIds = new TreeSet<>(); // Using TreeSet to maintain sorted order
            while (values.hasNext()) {
                documentIds.add(values.next());
            }
            output.put(
                    key, new ArrayList<>(documentIds)); // Emit (word, sorted list of document IDs)
        }
    }

    @ParameterizedTest
    @MethodSource("invertedIndexParameters")
    public void testInvertedIndex(InvertedIndexConfig testConfig) throws IOException {
        int inputFilesCount = testConfig.inputFilesCount;
        List<Path> inputFiles = generatesInputFiles(inputFilesCount, testConfig.DOCUMENTS);

        MapReduceJob<String, String, String, List<String>> job =
                new MapReduceJob<>(
                        new InvertedIndexMapper(),
                        new InvertedIndexReducer(),
                        null,
                        STRING_SERIALIZER,
                        STRING_SERIALIZER,
                        STRING_DESERIALIZER,
                        STRING_DESERIALIZER,
                        STRING_SERIALIZER,
                        LIST_SERIALIZER,
                        STRING_KEY_COMPARATOR,
                        STRING_KEY_HASH);

        Configuration config =
                new Configuration()
                        .set(MAPPERS_COUNT, testConfig.mappersCount)
                        .set(REDUCERS_COUNT, testConfig.reducersCount);

        // Измененный тип параметров
        MapReduceRunner mr = new MapReduceSequentialRunner();

        mr.run(job, inputFiles, config, mappersOutputPath, reducersOutputPath);

        Map<String, List<String>> reducesResult = new HashMap<>();
        for (int i = 0; i < testConfig.reducersCount; ++i) {
            readResult(reducersOutputPath.toString() + "/output-" + i + ".txt", reducesResult);
        }

        // Validate the output
        for (String word : testConfig.DOCUMENTS) {
            String[] expectedDocumentIdsArray = testConfig.expectedDocumentIds.get(word);
            List<String> expectedDocIds =
                    expectedDocumentIdsArray != null
                            ? Arrays.asList(expectedDocumentIdsArray)
                            : null; // Проверка на null
            List<String> actualDocIds = reducesResult.get(word);
        }
    }

    public static void readResult(String filename, Map<String, List<String>> result)
            throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2); // Разделяем на два элемента
                String word = parts[0];
                if (parts.length > 1) {
                    List<String> docIds = Arrays.asList(parts[1].split(","));
                    result.put(word, docIds);
                } else {
                    System.err.println(
                            "Warning: No document IDs found for word '"
                                    + word
                                    + "' in file '"
                                    + filename
                                    + "'");
                }
            }
        }
    }

    public static class InvertedIndexConfig {
        final String[] DOCUMENTS = {
            "doc1: apple banana orange",
            "doc2: banana kiwi",
            "doc3: apple banana",
            "doc4: orange grape",
        };

        final Map<String, String[]> expectedDocumentIds =
                new HashMap<>() {
                    {
                        put("apple", new String[] {"doc1", "doc3"});
                        put("banana", new String[] {"doc1", "doc2", "doc3"});
                        put("orange", new String[] {"doc1", "doc4"});
                        put("kiwi", new String[] {"doc2"});
                        put("grape", new String[] {"doc4"});
                    }
                };

        final int inputFilesCount = DOCUMENTS.length;
        final int mappersCount = 2; // Количество мапперов
        final int reducersCount = 2; // Количество редьюсеров

        @Override
        public String toString() {
            return "M = "
                    + mappersCount
                    + ", R = "
                    + reducersCount
                    + ", files = "
                    + inputFilesCount;
        }
    }

    static Stream<InvertedIndexConfig> invertedIndexParameters() {
        return Stream.of(new InvertedIndexConfig());
    }

    private static List<Path> generatesInputFiles(int inputFilesCount, String[] documents)
            throws IOException {
        List<Path> inputFiles = new ArrayList<>();
        for (int i = 0; i < inputFilesCount; i++) {
            Path tempFile = Files.createTempFile("input-", ".txt");
            try (BufferedWriter writer = Files.newBufferedWriter(tempFile)) {
                writer.write(documents[i]);
            }
            inputFiles.add(tempFile);
        }
        return inputFiles;
    }

    private static void deleteDirectory(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}
