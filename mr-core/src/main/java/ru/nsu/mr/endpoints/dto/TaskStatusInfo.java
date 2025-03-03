package ru.nsu.mr.endpoints.dto;

public record TaskStatusInfo(
        int jobId,
        int taskId,
        TaskType taskType,
        String status) {}
