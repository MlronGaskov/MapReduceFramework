package ru.nsu.mr.endpoints.dto;

public record NewJobDetails(
        int jobId,
        String jobPath,
        String jobStorageConnectionString,
        String inputsPath,
        String mappersOutputsPath,
        String reducersOutputsPath,
        String dataStorageConnectionString,
        int mappersCount,
        int reducersCount,
        int sorterInMemoryRecords) {}
