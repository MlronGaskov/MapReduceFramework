package ru.nsu.mr;

public interface JobLogger {
    void jobReceived(String jobId, String jobName);

    void jobStart(String jobId);

    void mapTaskFinish(String jobId);

    void reduceTaskFinish(String jobId);

    void jobFinish(String jobId);
}
