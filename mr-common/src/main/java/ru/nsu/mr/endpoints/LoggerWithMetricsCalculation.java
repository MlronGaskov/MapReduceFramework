package ru.nsu.mr.endpoints;

import ru.nsu.mr.JobLogger;
import ru.nsu.mr.endpoints.dto.JobDetails;
import ru.nsu.mr.endpoints.dto.JobState;
import ru.nsu.mr.endpoints.dto.JobSummary;

import java.util.*;
import java.util.stream.Collectors;

public class LoggerWithMetricsCalculation implements MetricsService, JobLogger {

    private static class Job {
        private final String jobId;
        private final String jobName;
        private int finishedMapTasksCount = 0;
        private int finishedReduceTasksCount = 0;
        private JobState state;

        public Job(String jobId, String jobName) {
            this.jobId = jobId;
            this.jobName = jobName;
            this.state = JobState.QUEUED;
        }

        public void start() {
            this.state = JobState.RUNNING;
        }

        public void complete() {
            this.state = JobState.COMPLETED;
        }

        public void mapTaskFinished() {
            finishedMapTasksCount += 1;
        }

        public void reduceTaskFinished() {
            finishedReduceTasksCount += 1;
        }
    }

    private final Map<String, Job> jobs = new HashMap<>();

    @Override
    public void jobReceived(String jobId, String jobName) {
        Job job = new Job(jobId, jobName);
        jobs.put(job.jobId, job);
    }

    @Override
    public void jobStart(String jobId) {
        jobs.get(jobId).start();
    }

    @Override
    public void mapTaskFinish(String jobId) {
        jobs.get(jobId).mapTaskFinished();
    }

    @Override
    public void reduceTaskFinish(String jobId) {
        jobs.get(jobId).reduceTaskFinished();
    }

    @Override
    public void jobFinish(String jobId) {
        jobs.get(jobId).complete();
    }

    @Override
    public List<JobSummary> getJobs() {
        return jobs.values().stream()
                .map(job -> new JobSummary(job.jobId, job.jobName, job.state))
                .collect(Collectors.toList());
    }

    @Override
    public JobDetails getJobDetails(String jobId) {
        Job job = jobs.get(jobId);
        if (job == null) return null;
        return new JobDetails(
                job.jobId,
                job.jobName,
                job.state,
                job.finishedMapTasksCount,
                job.finishedReduceTasksCount);
    }
}
