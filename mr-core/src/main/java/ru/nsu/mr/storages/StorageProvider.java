package ru.nsu.mr.storages;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface StorageProvider extends AutoCloseable {
    void get(String key, Path destination) throws IOException;
    void put(Path source, String key) throws IOException;
    List<String> list(String key) throws IOException;
}
