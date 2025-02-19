package ru.nsu.mr.sources;

import java.io.IOException;

public interface AutoCloseableSource extends AutoCloseable {
    @Override
    void close() throws IOException;
}