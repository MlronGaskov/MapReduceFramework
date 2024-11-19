package ru.nsu.mr;

import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.endpoints.*;
import ru.nsu.mr.endpoints.dto.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class Worker<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> {

    private static final class Task {
        public enum TaskStatus {
            RUNNING(null),
            SUCCEED(null),
            FAILED(null);

            private Exception exception;

            TaskStatus(Exception e) {
                this.exception = e;
            }

            private void setException(Exception e) {
                exception = e;
            }

            @Override
            public String toString() {
                return super.toString() + (exception != null ? ": " + exception.toString() : "");
            }
        }

        private final int taskId;
        private final TaskType taskType;
        private final List<Path> inputFiles;
        private TaskStatus status;

        private Task(int taskId, TaskType taskType, List<Path> inputFiles) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.inputFiles = inputFiles;
            this.status = TaskStatus.RUNNING;
        }

        private TaskInfo getTaskInfo() {
            return new TaskInfo(
                    taskId,
                    taskType,
                    inputFiles.stream().map(Object::toString).toList(),
                    status.toString());
        }

        private TaskDetails getTaskDetails() {
            return new TaskDetails(
                    taskId,
                    taskType,
                    inputFiles.stream().map(Object::toString).toList(),
                    status.toString());
        }
    }

    private final MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job;
    private final Configuration configuration;
    private final Path outputDirectory;
    private final Path mappersOutputPath;
    private volatile Task currentTask = null;
    private final Map<Integer, Task> previousTasks = new HashMap<>();

    public Worker(
            MapReduceJob<KEY_INTER, VALUE_INTER, KEY_OUT, VALUE_OUT> job,
            Configuration configuration,
            Path mappersOutputDirectory,
            Path outputDirectory,
            int serverPort)
            throws IOException {
        this.job = job;
        this.configuration = configuration;
        this.outputDirectory = outputDirectory;
        this.mappersOutputPath = mappersOutputDirectory;

        WorkerEndpoint workerEndpoint =
                new WorkerEndpoint(this::createTask, this::getTaskDetails, this::getAllTasks);
        workerEndpoint.startServer(serverPort);
    }

    public synchronized TaskDetails createTask(NewTaskDetails taskDetails) throws RuntimeException {
        if (currentTask != null) {
            throw new RuntimeException("the worker is busy");
        }
        currentTask =
                new Task(
                        taskDetails.taskId(),
                        taskDetails.taskType(),
                        taskDetails.inputFiles().stream().map(Path::of).toList());
        notify();
        return currentTask.getTaskDetails();
    }

    public synchronized TaskDetails getTaskDetails(int taskId) {
        if (currentTask != null && currentTask.taskId == taskId) {
            return currentTask.getTaskDetails();
        } else if (previousTasks.containsKey(taskId)) {
            return previousTasks.get(taskId).getTaskDetails();
        }
        return null;
    }

    public synchronized List<TaskInfo> getAllTasks() {
        List<TaskInfo> result =
                new ArrayList<>(previousTasks.values().stream().map(Task::getTaskInfo).toList());
        if (currentTask != null) {
            result.add(currentTask.getTaskInfo());
        }
        return result;
    }

    private synchronized void waitTask() throws InterruptedException {
        while (currentTask == null) {
            wait();
        }
    }

    private synchronized void moveCurrentTask() {
        previousTasks.put(currentTask.taskId, currentTask);
        currentTask = null;
    }

    public void start() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                waitTask();
                executeTask(currentTask);
                moveCurrentTask();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private synchronized void setCurrentTaskSucceed() {
        currentTask.status = Task.TaskStatus.SUCCEED;
    }

    private synchronized void setCurrentTaskFailed(Exception e) {
        currentTask.status = Task.TaskStatus.FAILED;
        currentTask.status.setException(e);
    }

    private void executeTask(Task task) {
        try {
            if (task.taskType.equals(TaskType.MAP)) {
                MapReduceTasksRunner.executeMapperTask(
                        task.inputFiles, task.taskId, mappersOutputPath, configuration, job);
            } else if (task.taskType.equals(TaskType.REDUCE)) {
                MapReduceTasksRunner.executeReduceTask(
                        task.inputFiles, task.taskId, outputDirectory, configuration, job);
            }
            setCurrentTaskSucceed();
        } catch (IOException e) {
            setCurrentTaskFailed(e);
        }
    }
}
