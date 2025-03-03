package ru.nsu.mr.endpoints.dto;

import java.util.List;

public record TaskInformation(
        int taskId,
        TaskType taskType,
        List<String> inputFiles,
        String targetDir,
        String storageConnectionString
) {}
