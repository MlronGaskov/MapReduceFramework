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
    public void download(String key, Path destination) {
        Path source = Path.of(key);
        try {
            if (destination.getParent() != null) {
                Files.createDirectories(destination.getParent());
            }
            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy file: " + key, e);
        }
    }

    @Override
    public void upload(Path source, String key) {
        Path destination = Path.of(key);
        try {
            if (destination.getParent() != null) {
                Files.createDirectories(destination.getParent());
            }
            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy file: " + source, e);
        }
    }

    @Override
    public List<String> list(String key) {
        Path directory = Path.of(key);
        try {
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
        } catch (IOException e) {
            throw new RuntimeException("Failed to list files in directory: " + key, e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
