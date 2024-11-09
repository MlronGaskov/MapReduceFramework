package ru.nsu.mr;

public interface Logger {
    void jobReceived(String jobId, String jobName);

    void jobStart(String jobId);

    void mapTaskFinish(String jobId);

    void reduceTaskFinish(String jobId);

    void jobFinish(String jobId);
}
