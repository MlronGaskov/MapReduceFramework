package ru.nsu.mr.endpoints.dto;

public record JobSummary(String jobId, String jobName, JobState state) {}
