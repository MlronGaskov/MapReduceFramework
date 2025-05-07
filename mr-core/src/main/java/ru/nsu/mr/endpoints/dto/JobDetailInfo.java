package ru.nsu.mr.endpoints.dto;

import java.util.List;

public record JobDetailInfo(
        String jobStorageConnectionString,
        String dataStorageConnectionString,
        String inputsPath,
        String reducersOutputsPath,
        int mappersCount,
        int reducersCount,
        JobProgressInfo progressInfo
) {}