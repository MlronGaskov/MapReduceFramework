package ru.nsu.mr;


import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.nsu.mr.config.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static ru.nsu.mr.config.ConfigurationOption.MAPPERS_COUNT;
import static ru.nsu.mr.config.ConfigurationOption.REDUCERS_COUNT;

public class WebLinkGraphITCase {

    static class ReverseWebLinkMapper implements Mapper<String, String, String, String> {
        @Override
        public void map(Iterator<Pair<String, String>> input, OutputContext<String, String> output) {
            while (input.hasNext()) {
                Pair<String, String> fileAndLinks = input.next();
                String[] links = fileAndLinks.value().split("\\s+");
                output.put(links[1], links[0]);
            }
        }
    }

    static class ReverseWebLinkReducer implements Reducer<String, String, String, List<String>> {
        @Override
        public void reduce(String key, Iterator<String> values, OutputContext<String, List<String>> output) {
            List<String> sources = new ArrayList<>();
            while (values.hasNext()) {
                String value = values.next();
                if(!sources.contains(value)){
                    sources.add(value);
                }
            }
            output.put(key, sources);
        }
    }
    @ParameterizedTest
    @MethodSource ("webLinkGraphParameters")
    public void testReverseWebLinkGraph(WebLinkGraphSettings testSettings) throws IOException {
        Path inputFilesPath = Path.of(testSettings.inputFilesPath);
        Path mappersOutputPath = Path.of(testSettings.mappersOutputPath);
        Path reducersOutputPath = Path.of(testSettings.reducersOutputPath);
        Path mapperAnswersPath = Path.of(testSettings.mapperAnswersPath);
        Path outputAnswersPath = Path.of(testSettings.outputAnswersPath);


        List<Path> inputFiles = Files.list(inputFilesPath).toList();

        MapReduceJob<String, String, String, List<String>> job = new MapReduceJob<>(
                new ReverseWebLinkMapper(),
                new ReverseWebLinkReducer(),
                x -> x,
                x -> x,
                x -> x,
                x -> x,
                x -> x,
                Object::toString,
                String::compareTo,
                String::hashCode

        );

        Configuration config = new Configuration()
                .set(MAPPERS_COUNT, testSettings.mappersCount)
                .set(REDUCERS_COUNT, testSettings.reducersCount);

        MapReduceRunner<String, String, String, List<String>> mr = new MapReduceSequentialRunner<>();
        mr.run(job, inputFiles, config, mappersOutputPath, reducersOutputPath);

        List<Path> mapperFiles = List.of(Files.list(mappersOutputPath).toArray(Path[]::new));
        List<Path> mapperAnswerFiles = List.of(Files.list(mapperAnswersPath).toArray(Path[]::new));
        assertEquals(mapperFiles.size(), mapperAnswerFiles.size());

        for (int i= 0; i<mapperFiles.size(); i++) {
            compareFileContents(mapperFiles.get(i), mapperAnswerFiles.get(i));
        }

        List<Path> reducerFiles = List.of(Files.list(reducersOutputPath).toArray(Path[]::new));
        List<Path> outputAnswerFiles = List.of(Files.list(outputAnswersPath).toArray(Path[]::new));
        assertEquals(reducerFiles.size(), outputAnswerFiles.size());

        for (int i= 0; i<reducerFiles.size(); i++) {
            compareFileContents(reducerFiles.get(i), outputAnswerFiles.get(i));
        }

    }

    public static class WebLinkGraphSettings {
        int mappersCount;
        int reducersCount;
        String inputFilesPath;
        String mappersOutputPath;
        String reducersOutputPath;
        String mapperAnswersPath;
        String outputAnswersPath;

        WebLinkGraphSettings(int mappersCount, int reducersCount, String inputFilesPath,
                             String mappersOutputPath, String reducersOutputPath, String mapperAnswersPath,
                             String outputAnswersPath) throws IOException {
            this.mappersCount = mappersCount;
            this.reducersCount = reducersCount;
            this.inputFilesPath = inputFilesPath;
            this.mappersOutputPath = mappersOutputPath;
            this.reducersOutputPath = reducersOutputPath;
            this.mapperAnswersPath = mapperAnswersPath;
            this.outputAnswersPath = outputAnswersPath;

            clearDirectory(mappersOutputPath);
            clearDirectory(reducersOutputPath);
        }
    }

    static Stream<WebLinkGraphSettings> webLinkGraphParameters() throws IOException {
        return Stream.of(
                new WebLinkGraphSettings(3, 3,"src/test/resources/WebLinkGraphITCase/InputFiles",
                        "src/test/resources/WebLinkGraphITCase/MapperOutputFiles",
                        "src/test/resources/WebLinkGraphITCase/OutputFiles",
                        "src/test/resources/WebLinkGraphITCase/AnswerFiles/Mapper",
                        "src/test/resources/WebLinkGraphITCase/AnswerFiles/Output")
        );
    }

    private void compareFileContents(Path file1, Path file2) throws IOException {
        try (BufferedReader reader1 = Files.newBufferedReader(file1);
             BufferedReader reader2 = Files.newBufferedReader(file2)) {

            String line1 = reader1.readLine();
            String line2 = reader2.readLine();

            while (line1 != null || line2 != null) {
                assertEquals(line1, line2);
                line1 = reader1.readLine();
                line2 = reader2.readLine();
            }
        }
    }

    private static void clearDirectory(String directoryPath) throws IOException {
        Path path = Paths.get(directoryPath);
        try (Stream<Path> pathStream = Files.walk(path)) {
            pathStream.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        if (!p.equals(path)){
                            try {
                                Files.delete(p);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        }
    }
}