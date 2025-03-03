package ru.nsu.mr.endpoints.dto;

public record JobInformation(
        int jobId,
        String userJobPath,
        String storageConnectionString,
        int mappersCount,
        int reducersCount,
        int sorterInMemoryRecords) {}
