package ru.nsu.mr.endpoints.data;

public record JobInfo(String date, String status, int completedMapJobs, int completedReduceJobs) {}
