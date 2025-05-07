package ru.nsu.mr.endpoints.dto;


public record JobQueueInfo(
        Integer jobId,
        String submissionTime
) {}
