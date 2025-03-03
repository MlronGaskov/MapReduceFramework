package ru.nsu.mr;

import ru.nsu.mr.storages.StorageProvider;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class TemporaryDirectory implements AutoCloseable {
    private final Path tempDir;
    private final StorageProvider storageProvider;

    public TemporaryDirectory(StorageProvider storageProvider) throws IOException {
        this.tempDir = Files.createTempDirectory("tempDir");
        this.storageProvider = storageProvider;
    }

    public Path getPath() {
        return tempDir;
    }

    public List<Path> get(List<String> keys, String destination) throws IOException {
        Path destinationDir = tempDir.resolve(destination);
        if (!Files.exists(destinationDir)) {
            Files.createDirectories(destinationDir);
        }
        List<Path> localPaths = new ArrayList<>();
        for (String key : keys) {
            Path fileName = Paths.get(key).getFileName();
            if (fileName == null) {
                throw new IllegalArgumentException("Unable to extract file name from key: " + key);
            }
            Path destinationPath = destinationDir.resolve(fileName);
            storageProvider.get(key, destinationPath);
            localPaths.add(destinationPath);
        }
        return localPaths;
    }

    public void put(Path sourceDirectory, String keyPrefix) throws IOException {
        try (Stream<Path> stream = Files.walk(sourceDirectory)) {
            stream.filter(Files::isRegularFile)
                    .forEach(file -> {
                        Path relativePath = sourceDirectory.relativize(file);
                        String key = keyPrefix + "/" + relativePath.toString().replace(FileSystems.getDefault().getSeparator(), "/");
                        try {
                            storageProvider.put(file, key);
                        } catch (IOException e) {
                            throw new RuntimeException("Error uploading file: " + file, e);
                        }
                    });
        }
    }

    @Override
    public void close() throws IOException {
        if (Files.exists(tempDir)) {
            try (Stream<Path> stream = Files.walk(tempDir)) {
                stream.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                System.err.println("Failed to delete " + path + ": " + e.getMessage());
                            }
                        });
            }
        }
    }
}
