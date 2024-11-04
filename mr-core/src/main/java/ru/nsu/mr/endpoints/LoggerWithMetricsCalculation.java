package ru.nsu.mr.endpoints;

import ru.nsu.mr.Logger;
import ru.nsu.mr.endpoints.data.Job;
import ru.nsu.mr.endpoints.data.JobInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class LoggerWithMetricsCalculation implements MetricsService, Logger {
    private static class JobEntry {
        private final String jobStartDate;
        private String status;
        private int finishedMapTasksCount = 0;
        private int finishedReduceTasksCount = 0;

        public JobEntry(String date) {
            this.jobStartDate = date;
            this.status = "PENDING";
        }

        public void start() {
            status = "IN_PROGRESS";
        }

        public void finished() {
            status = "COMPLETED";
        }

        public void mapTaskFinished() {
            finishedMapTasksCount += 1;
        }

        public void reduceTaskFinished() {
            finishedReduceTasksCount += 1;
        }
    }

    List<JobEntry> jobs = new ArrayList<>();

    @Override
    public void jobAdd(String jobStartDate) {
        jobs.add(new JobEntry(jobStartDate));
    }

    @Override
    public void jobStart(String jobStartDate) {
        for (JobEntry job : jobs) {
            if (Objects.equals(job.jobStartDate, jobStartDate)) {
                job.start();
            }
        }
    }

    @Override
    public void mapTaskStart(String jobStartDate, int taskId) {}

    @Override
    public void mapTaskFinish(String jobStartDate, int taskId) {
        for (JobEntry job : jobs) {
            if (Objects.equals(job.jobStartDate, jobStartDate)) {
                job.mapTaskFinished();
            }
        }
    }

    @Override
    public void reduceTaskStart(String jobStartDate, int taskId) {}

    @Override
    public void reduceTaskFinish(String jobStartDate, int taskId) {
        for (JobEntry job : jobs) {
            if (Objects.equals(job.jobStartDate, jobStartDate)) {
                job.reduceTaskFinished();
            }
        }
    }

    @Override
    public void jobFinish(String jobStartDate) {
        for (JobEntry job : jobs) {
            if (Objects.equals(job.jobStartDate, jobStartDate)) {
                job.finished();
            }
        }
    }

    @Override
    public List<Job> getJobs() {
        List<Job> jobsToReturn = new ArrayList<>();
        for (JobEntry job : jobs) {
            jobsToReturn.add(new Job(job.jobStartDate, job.status));
        }
        jobsToReturn.sort(Comparator.comparing(Job::date));
        return jobsToReturn;
    }

    @Override
    public JobInfo getJobInfo(String jobStartDate) {
        for (JobEntry job : jobs) {
            if (Objects.equals(job.jobStartDate, jobStartDate)) {
                return new JobInfo(
                        job.jobStartDate,
                        job.status,
                        job.finishedMapTasksCount,
                        job.finishedReduceTasksCount);
            }
        }
        return null;
    }
}
