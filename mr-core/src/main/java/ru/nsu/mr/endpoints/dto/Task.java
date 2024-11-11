package ru.nsu.mr.endpoints.dto;

import ru.nsu.mr.endpoints.WorkerManager;

import java.nio.file.Path;
import java.util.List;

public class Task {
    private int taskId;
    private String taskType;
    private List<String> inputFiles;

    public int getTaskId() {
        return taskId;
    }

    public WorkerManager.TaskType getTaskType() {
        return WorkerManager.TaskType.valueOf(taskType);
    }

    public List<Path> getInputFiles() {
        return inputFiles.stream().map(Path::of).toList();
    }
}
