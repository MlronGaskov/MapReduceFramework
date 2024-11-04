package ru.nsu.mr.endpoints;

import ru.nsu.mr.endpoints.data.Job;
import ru.nsu.mr.endpoints.data.JobInfo;

import java.util.List;

public interface MetricsService {
    List<Job> getJobs();

    JobInfo getJobInfo(String dateTime);
}
