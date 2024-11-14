package ru.nsu.mr.endpoints.dto;

import java.nio.file.Path;
import java.util.List;

public class NewTaskDetails {
    private final int taskId;
    private final TaskType taskType;
    private final List<Path> inputFiles;

    public NewTaskDetails(int taskId, TaskType taskType, List<Path> inputFiles) {
        this.taskId = taskId;
        this.taskType = taskType;
        this.inputFiles = inputFiles;
    }

    public int getTaskId() {
        return taskId;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public List<Path> getInputFiles() {
        return inputFiles;
    }
}
