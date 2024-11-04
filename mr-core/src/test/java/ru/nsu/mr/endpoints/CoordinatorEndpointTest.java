package ru.nsu.mr.endpoints;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.Gson;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import ru.nsu.mr.endpoints.data.Job;
import ru.nsu.mr.endpoints.data.JobInfo;

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
                            public List<Job> getJobs() {
                                return List.of(
                                        new Job(
                                                LocalDateTime.of(2024, 1, 1, 1, 1).toString(),
                                                "PENDING"),
                                        new Job(
                                                LocalDateTime.of(2024, 1, 1, 2, 1).toString(),
                                                "IN_PROGRESS"),
                                        new Job(
                                                LocalDateTime.of(2024, 1, 1, 3, 1).toString(),
                                                "FAILED"),
                                        new Job(
                                                LocalDateTime.of(2024, 1, 1, 4, 1).toString(),
                                                "CANCELLED"),
                                        new Job(
                                                LocalDateTime.of(2024, 1, 2, 1, 1).toString(),
                                                "COMPLETED"));
                            }

                            @Override
                            public JobInfo getJobInfo(String dateTime) {
                                return switch (dateTime) {
                                    case "2024-01-01T01:01" ->
                                            new JobInfo(dateTime, "PENDING", 0, 0);
                                    case "2024-01-01T02:01" ->
                                            new JobInfo(dateTime, "IN_PROGRESS", 4, 2);
                                    case "2024-01-01T03:01" ->
                                            new JobInfo(dateTime, "FAILED", 3, 1);
                                    case "2024-01-01T04:01" ->
                                            new JobInfo(dateTime, "CANCELLED", 0, 0);
                                    case "2024-01-02T01:01" ->
                                            new JobInfo(dateTime, "COMPLETED", 10, 10);
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

        List<Job> expectedJobs =
                List.of(
                        new Job(LocalDateTime.of(2024, 1, 1, 1, 1).toString(), "PENDING"),
                        new Job(LocalDateTime.of(2024, 1, 1, 2, 1).toString(), "IN_PROGRESS"),
                        new Job(LocalDateTime.of(2024, 1, 1, 3, 1).toString(), "FAILED"),
                        new Job(LocalDateTime.of(2024, 1, 1, 4, 1).toString(), "CANCELLED"),
                        new Job(LocalDateTime.of(2024, 1, 2, 1, 1).toString(), "COMPLETED"));

        assertEquals(
                gson.toJson(expectedJobs), response, "Job list did not match expected response");
    }

    @Test
    public void testJobGet() throws IOException, URISyntaxException {
        List<JobInfo> testCases =
                List.of(
                        new JobInfo("2024-01-01T01:01", "PENDING", 0, 0),
                        new JobInfo("2024-01-01T02:01", "IN_PROGRESS", 4, 2),
                        new JobInfo("2024-01-01T03:01", "FAILED", 3, 1),
                        new JobInfo("2024-01-01T04:01", "CANCELLED", 0, 0),
                        new JobInfo("2024-01-02T01:01", "COMPLETED", 10, 10));

        for (JobInfo expectedJobInfo : testCases) {
            URI uri = new URI("http://localhost:8080/jobs/" + expectedJobInfo.date());
            HttpURLConnection connection = openConnection(uri);

            assertEquals(
                    200,
                    connection.getResponseCode(),
                    "Expected HTTP 200 for date: " + expectedJobInfo.date());

            String response = getResponseContent(connection);
            assertEquals(
                    gson.toJson(expectedJobInfo),
                    response,
                    "Response did not match for date: " + expectedJobInfo.date());
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
