package ru.nsu.mr.endpoints;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import ru.nsu.mr.Logger;
import ru.nsu.mr.endpoints.data.Job;
import ru.nsu.mr.endpoints.data.JobInfo;

import java.util.List;

public class LoggerWithMetricsCalculationTest {
    @Test
    public void test() {
        String job1date = "2024-01-01T01:01";
        String job2date = "2024-01-01T02:01";
        String job3date = "2024-01-01T03:01";

        Logger logger = new LoggerWithMetricsCalculation();
        MetricsService metricsService = (MetricsService) logger;

        logger.jobAdd(job1date);
        logger.jobStart(job1date);
        logger.mapTaskStart(job1date, 0);
        logger.mapTaskFinish(job1date, 0);
        logger.reduceTaskStart(job1date, 0);
        logger.reduceTaskFinish(job1date, 0);
        logger.jobFinish(job1date);

        JobInfo job1Info = metricsService.getJobInfo(job1date);
        assertNotNull(job1Info);
        assertEquals("COMPLETED", job1Info.status());
        assertEquals(1, job1Info.completedMapJobs());
        assertEquals(1, job1Info.completedReduceJobs());

        logger.jobAdd(job2date);
        logger.jobAdd(job3date);

        logger.jobStart(job2date);
        logger.jobStart(job3date);

        logger.mapTaskStart(job2date, 0);
        logger.mapTaskStart(job2date, 1);
        logger.mapTaskStart(job2date, 2);
        logger.mapTaskStart(job3date, 0);
        logger.mapTaskFinish(job2date, 0);
        logger.mapTaskFinish(job2date, 1);
        logger.mapTaskStart(job2date, 3);
        logger.mapTaskStart(job3date, 1);
        logger.mapTaskFinish(job2date, 3);
        logger.mapTaskFinish(job2date, 2);
        logger.mapTaskFinish(job3date, 0);
        logger.mapTaskFinish(job3date, 1);

        logger.reduceTaskStart(job2date, 0);
        logger.reduceTaskStart(job2date, 1);

        logger.reduceTaskStart(job3date, 0);
        logger.reduceTaskStart(job3date, 1);
        logger.reduceTaskFinish(job3date, 1);
        logger.reduceTaskFinish(job3date, 0);
        logger.jobFinish(job3date);

        JobInfo job2Info = metricsService.getJobInfo(job2date);
        assertNotNull(job2Info);
        assertEquals("IN_PROGRESS", job2Info.status());
        assertEquals(4, job2Info.completedMapJobs());
        assertEquals(0, job2Info.completedReduceJobs());

        JobInfo job3Info = metricsService.getJobInfo(job3date);
        assertNotNull(job3Info);
        assertEquals("COMPLETED", job3Info.status());
        assertEquals(2, job3Info.completedMapJobs());
        assertEquals(2, job3Info.completedReduceJobs());

        logger.reduceTaskFinish(job2date, 1);
        logger.reduceTaskFinish(job2date, 0);
        logger.jobFinish(job2date);

        logger.reduceTaskFinish(job2date, 1);
        logger.jobFinish(job2date);
        job2Info = metricsService.getJobInfo(job2date);
        assertEquals("COMPLETED", job2Info.status());

        List<Job> jobs = metricsService.getJobs();
        assertEquals(3, jobs.size());
        assertEquals(job1date, jobs.get(0).date());
        assertEquals(job2date, jobs.get(1).date());
        assertEquals(job3date, jobs.get(2).date());
    }
}
