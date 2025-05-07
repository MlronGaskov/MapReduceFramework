package ru.nsu.mr.endpoints.dto;

import java.util.List;

public record JobProgressInfo(
        String status,
        String terminationStatus,
        String phase,
        Integer totalTasks,
        Integer completedTasks,
        List<PhaseDuration> phaseDurations
) {}
