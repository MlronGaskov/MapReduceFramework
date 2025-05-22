package ru.nsu.mr.endpoints.dto;

public record PhaseDuration(
        String phaseName,
        String start,
        String end
) {}
