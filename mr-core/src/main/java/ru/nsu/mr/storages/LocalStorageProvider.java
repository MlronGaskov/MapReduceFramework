package ru.nsu.mr.storages;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalStorageProvider implements StorageProvider {

    @Override
    public void get(String key, Path destination) throws IOException {
        Path source = Path.of(key);
        if (destination.getParent() != null) {
            Files.createDirectories(destination.getParent());
        }
        Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void put(Path source, String key) throws IOException {
        Path destination = Path.of(key);
        if (destination.getParent() != null) {
            Files.createDirectories(destination.getParent());
        }
        Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public List<String> list(String key) throws IOException {
        Path directory = Path.of(key);
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
            return List.of();
        } else if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("Provided key is not a directory: " + key);
        }
        try (Stream<Path> stream = Files.list(directory)) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(Path::toString)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public long getFileSize(String key) throws IOException {
        Path filePath = Path.of(key);
        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + key);
        }
        return Files.size(filePath);
    }
}
