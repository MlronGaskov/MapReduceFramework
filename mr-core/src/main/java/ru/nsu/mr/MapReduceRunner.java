package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;

import java.nio.file.Path;
import java.util.List;

public interface MapReduceRunner<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> {
    void run(
            MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job,
            List<Path> inputFiles,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory);
}
