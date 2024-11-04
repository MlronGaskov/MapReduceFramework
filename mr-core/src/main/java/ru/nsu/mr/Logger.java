package ru.nsu.mr;

public interface Logger {
    void jobAdd(String jobStartDate);

    void jobStart(String jobStartDate);

    void mapTaskStart(String jobStartDate, int taskId);

    void mapTaskFinish(String jobStartDate, int taskId);

    void reduceTaskStart(String jobStartDate, int taskId);

    void reduceTaskFinish(String jobStartDate, int taskId);

    void jobFinish(String jobStartDate);
}
