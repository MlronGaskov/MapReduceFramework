package ru.nsu.mr.endpoints.dto;

import java.util.List;

public record TaskDetails(int taskId, TaskType taskType, List<String> inputFiles, String status) {}
