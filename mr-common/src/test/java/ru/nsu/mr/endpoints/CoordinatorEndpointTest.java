package ru.nsu.mr.endpoints;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.Gson;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import ru.nsu.mr.endpoints.dto.JobDetails;
import ru.nsu.mr.endpoints.dto.JobState;
import ru.nsu.mr.endpoints.dto.JobSummary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.List;

public class CoordinatorEndpointTest {

    private static final Gson gson = new Gson();
    private static CoordinatorEndpoint endpoint;

    @BeforeAll
    public static void setup() throws IOException {
        endpoint = new CoordinatorEndpoint(
                "8080",
                new MockMetricsService(),
                workerPort -> {},
                taskDetails -> {}
        );
        endpoint.startServer();
    }

    @AfterAll
    public static void tearDown() {
        endpoint.stopServer();
    }

    @Test
    public void testGetJobs() throws IOException, URISyntaxException {
        URI uri = new URI("http://localhost:8080/jobs");
        HttpURLConnection connection = openConnection(uri);

        assertEquals(200, connection.getResponseCode(), "Expected HTTP 200 for GET /jobs");

        String response = getResponseContent(connection);
        List<JobSummary> expectedJobSummaries = MockMetricsService.EXPECTED_JOB_SUMMARIES;

        assertEquals(
                gson.toJson(expectedJobSummaries),
                response,
                "The job summaries returned by the endpoint do not match the expected values."
        );
    }

    @Test
    public void testGetJobDetails() throws IOException, URISyntaxException {
        for (JobDetails expectedDetails : MockMetricsService.EXPECTED_JOB_DETAILS) {
            URI uri = new URI("http://localhost:8080/jobs/" + expectedDetails.jobId());
            HttpURLConnection connection = openConnection(uri);

            assertEquals(200, connection.getResponseCode(),
                    "Expected HTTP 200 for GET /jobs/" + expectedDetails.jobId());

            String response = getResponseContent(connection);

            assertEquals(
                    gson.toJson(expectedDetails),
                    response,
                    "The job details returned by the endpoint do not match the expected values for job ID: "
                            + expectedDetails.jobId());
        }
    }

    private HttpURLConnection openConnection(URI uri) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        return connection;
    }

    private String getResponseContent(HttpURLConnection connection) throws IOException {
        StringBuilder response = new StringBuilder();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        }
        return response.toString();
    }

    private static class MockMetricsService implements MetricsService {
        static final List<JobSummary> EXPECTED_JOB_SUMMARIES = List.of(
                new JobSummary("1", LocalDateTime.of(2024, 1, 1, 1, 1).toString(), JobState.QUEUED),
                new JobSummary("2", LocalDateTime.of(2024, 1, 1, 2, 1).toString(), JobState.RUNNING),
                new JobSummary("3", LocalDateTime.of(2024, 1, 1, 3, 1).toString(), JobState.FAILED),
                new JobSummary("4", LocalDateTime.of(2024, 1, 2, 1, 1).toString(), JobState.COMPLETED)
        );

        static final List<JobDetails> EXPECTED_JOB_DETAILS = List.of(
                new JobDetails("1", "1", JobState.QUEUED, 0, 0),
                new JobDetails("2", "2", JobState.RUNNING, 4, 2),
                new JobDetails("3", "3", JobState.FAILED, 3, 1),
                new JobDetails("4", "4", JobState.COMPLETED, 10, 10)
        );

        @Override
        public List<JobSummary> getJobs() {
            return EXPECTED_JOB_SUMMARIES;
        }

        @Override
        public JobDetails getJobDetails(String jobId) {
            return EXPECTED_JOB_DETAILS.stream()
                    .filter(details -> details.jobId().equals(jobId))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Job not found for ID: " + jobId));
        }
    }
}
