package ru.nsu.mr.endpoints.dto;

public record NewTaskDetails(
        JobInformation jobInformation,
        TaskInformation taskInformation) {}
