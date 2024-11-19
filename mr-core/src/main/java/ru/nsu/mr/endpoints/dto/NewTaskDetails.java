package ru.nsu.mr.endpoints.dto;

import java.util.List;

public record NewTaskDetails(int taskId, TaskType taskType, List<String> inputFiles) {}
