package ru.nsu.mr.endpoints;

import ru.nsu.mr.endpoints.dto.JobDetails;
import ru.nsu.mr.endpoints.dto.JobSummary;

import java.util.List;

public interface MetricsService {
    List<JobSummary> getJobs();

    JobDetails getJobDetails(String jobId);
}
