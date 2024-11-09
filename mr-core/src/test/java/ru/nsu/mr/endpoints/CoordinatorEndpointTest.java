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
        endpoint =
                new CoordinatorEndpoint(
                        "8080",
                        new MetricsService() {
                            @Override
                            public List<JobSummary> getJobs() {
                                return List.of(
                                        new JobSummary(
                                                "1",
                                                LocalDateTime.of(2024, 1, 1, 1, 1).toString(),
                                                JobState.QUEUED),
                                        new JobSummary(
                                                "2",
                                                LocalDateTime.of(2024, 1, 1, 2, 1).toString(),
                                                JobState.RUNNING),
                                        new JobSummary(
                                                "3",
                                                LocalDateTime.of(2024, 1, 1, 3, 1).toString(),
                                                JobState.FAILED),
                                        new JobSummary(
                                                "4",
                                                LocalDateTime.of(2024, 1, 2, 1, 1).toString(),
                                                JobState.COMPLETED));
                            }

                            @Override
                            public JobDetails getJobDetails(String dateTime) {
                                return switch (dateTime) {
                                    case "1" ->
                                            new JobDetails("1", dateTime, JobState.QUEUED, 0, 0);
                                    case "2" ->
                                            new JobDetails("2", dateTime, JobState.RUNNING, 4, 2);
                                    case "3" ->
                                            new JobDetails("3", dateTime, JobState.FAILED, 3, 1);
                                    case "4" ->
                                            new JobDetails(
                                                    "4", dateTime, JobState.COMPLETED, 10, 10);
                                    default -> throw new IllegalArgumentException();
                                };
                            }
                        });
        endpoint.start();
    }

    @AfterAll
    public static void tearDown() {
        endpoint.stop();
    }

    @Test
    public void testJobsGet() throws IOException, URISyntaxException {
        URI uri = new URI("http://localhost:8080/jobs");
        HttpURLConnection connection = openConnection(uri);

        String response = getResponseContent(connection);
        assertEquals(200, connection.getResponseCode(), "Expected HTTP 200 for /jobs");

        List<JobSummary> expectedJobSummaries =
                List.of(
                        new JobSummary(
                                "1",
                                LocalDateTime.of(2024, 1, 1, 1, 1).toString(),
                                JobState.QUEUED),
                        new JobSummary(
                                "2",
                                LocalDateTime.of(2024, 1, 1, 2, 1).toString(),
                                JobState.RUNNING),
                        new JobSummary(
                                "3",
                                LocalDateTime.of(2024, 1, 1, 3, 1).toString(),
                                JobState.FAILED),
                        new JobSummary(
                                "4",
                                LocalDateTime.of(2024, 1, 2, 1, 1).toString(),
                                JobState.COMPLETED));

        assertEquals(
                gson.toJson(expectedJobSummaries),
                response,
                "Job list did not match expected response");
    }

    @Test
    public void testJobGet() throws IOException, URISyntaxException {
        List<JobDetails> testCases =
                List.of(
                        new JobDetails("1", "1", JobState.QUEUED, 0, 0),
                        new JobDetails("2", "2", JobState.RUNNING, 4, 2),
                        new JobDetails("3", "3", JobState.FAILED, 3, 1),
                        new JobDetails("4", "4", JobState.COMPLETED, 10, 10));

        for (JobDetails expectedJobDetails : testCases) {
            URI uri = new URI("http://localhost:8080/jobs/" + expectedJobDetails.jobId());
            HttpURLConnection connection = openConnection(uri);
            assertEquals(
                    200,
                    connection.getResponseCode(),
                    "Expected HTTP 200 for date: " + expectedJobDetails.jobId());

            String response = getResponseContent(connection);
            assertEquals(
                    gson.toJson(expectedJobDetails),
                    response,
                    "Response did not match for date: " + expectedJobDetails.jobId());
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
        try (BufferedReader in =
                new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        }
        return response.toString();
    }
}
