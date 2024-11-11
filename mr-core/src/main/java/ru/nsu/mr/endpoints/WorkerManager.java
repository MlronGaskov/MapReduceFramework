package ru.nsu.mr.endpoints;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerManager {
    private Task currentTask = null;
    private final AtomicBoolean isTaskRunning = new AtomicBoolean(false);

    public synchronized Optional<String> addTask(int id, TaskType type, List<Path> inputFiles) {
        if (isTaskRunning.get()) {
            return Optional.of("Error: Task already in progress");
        }
        currentTask = new Task(id, type, inputFiles);
        notifyAll();
        isTaskRunning.set(true);
        return Optional.empty();
    }

    public synchronized Optional<Task> getCurrentTask() {
        if (currentTask != null && isTaskRunning.get()) {
            return Optional.of(currentTask);
        }
        return Optional.empty();
    }

    public synchronized void completeCurrentTask() {
        isTaskRunning.set(false);
        currentTask = null;
    }

    public static class Task {
        private final TaskType type;
        private final List<Path> inputFiles;
        private final int taskId;

        public Task(int taskId, TaskType type, List<Path> inputFiles) {
            this.type = type;
            this.inputFiles = inputFiles;
            this.taskId = taskId;
        }

        public int getTaskId() {
            return taskId;
        }

        public TaskType getType() {
            return type;
        }

        public List<Path> getInputFiles() {
            return inputFiles;
        }
    }

    public enum TaskType {
        MAP,
        REDUCE
    }
}
