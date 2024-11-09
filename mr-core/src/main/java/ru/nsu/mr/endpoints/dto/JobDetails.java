package ru.nsu.mr.endpoints.dto;

public record JobDetails(
        String jobId,
        String jobName,
        JobState state,
        int completedMapJobs,
        int completedReduceJobs) {}
