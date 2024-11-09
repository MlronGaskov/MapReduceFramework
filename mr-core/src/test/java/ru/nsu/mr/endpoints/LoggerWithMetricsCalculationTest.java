package ru.nsu.mr.endpoints;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import ru.nsu.mr.Logger;
import ru.nsu.mr.endpoints.dto.JobDetails;
import ru.nsu.mr.endpoints.dto.JobState;
import ru.nsu.mr.endpoints.dto.JobSummary;

import java.util.List;

public class LoggerWithMetricsCalculationTest {
    @Test
    public void test() {
        String job1id = "2024-01-01T01:01";
        String job2id = "2024-01-01T02:01";
        String job3id = "2024-01-01T03:01";

        Logger logger = new LoggerWithMetricsCalculation();
        MetricsService metricsService = (MetricsService) logger;

        logger.jobReceived(job1id, job1id);
        logger.jobStart(job1id);
        logger.mapTaskFinish(job1id);
        logger.reduceTaskFinish(job1id);
        logger.jobFinish(job1id);

        JobDetails job1Info = metricsService.getJobDetails(job1id);
        assertNotNull(job1Info);
        assertEquals(JobState.COMPLETED, job1Info.state());
        assertEquals(1, job1Info.completedMapJobs());
        assertEquals(1, job1Info.completedReduceJobs());

        logger.jobReceived(job2id, job2id);
        logger.jobReceived(job3id, job3id);

        logger.jobStart(job2id);
        logger.jobStart(job3id);

        logger.mapTaskFinish(job2id);
        logger.mapTaskFinish(job2id);
        logger.mapTaskFinish(job2id);
        logger.mapTaskFinish(job2id);
        logger.mapTaskFinish(job3id);
        logger.mapTaskFinish(job3id);
        logger.reduceTaskFinish(job3id);
        logger.reduceTaskFinish(job3id);
        logger.jobFinish(job3id);

        JobDetails job2Info = metricsService.getJobDetails(job2id);
        assertNotNull(job2Info);
        assertEquals(JobState.RUNNING, job2Info.state());
        assertEquals(4, job2Info.completedMapJobs());
        assertEquals(0, job2Info.completedReduceJobs());

        JobDetails job3Info = metricsService.getJobDetails(job3id);
        assertNotNull(job3Info);
        assertEquals(JobState.COMPLETED, job3Info.state());
        assertEquals(2, job3Info.completedMapJobs());
        assertEquals(2, job3Info.completedReduceJobs());

        logger.reduceTaskFinish(job2id);
        logger.reduceTaskFinish(job2id);
        logger.jobFinish(job2id);

        logger.reduceTaskFinish(job2id);
        logger.jobFinish(job2id);
        job2Info = metricsService.getJobDetails(job2id);
        assertEquals(JobState.COMPLETED, job2Info.state());

        List<JobSummary> jobSummaries = metricsService.getJobs();
        assertEquals(3, jobSummaries.size());
        assertEquals(job1id, jobSummaries.get(0).jobId());
        assertEquals(job2id, jobSummaries.get(1).jobId());
        assertEquals(job3id, jobSummaries.get(2).jobId());
    }
}
