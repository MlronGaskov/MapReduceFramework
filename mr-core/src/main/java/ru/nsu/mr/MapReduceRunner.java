package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;

import java.nio.file.Path;
import java.util.List;

public interface MapReduceRunner {
    void run(
            MapReduceJob<?, ?, ?, ?> job,
            List<Path> inputFiles,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory);
}
