package ru.nsu.mr.endpoints.dto;

public record TaskDetails(
        int jobId,
        TaskInformation taskInformation,
        String status) {}
