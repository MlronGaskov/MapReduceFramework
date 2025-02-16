package ru.nsu.mr.storages;

import java.nio.file.Path;
import java.util.List;

public interface StorageProvider extends AutoCloseable {
    void download(String key, Path destination);
    void upload(Path source, String key);
    List<String> list(String key);
}
