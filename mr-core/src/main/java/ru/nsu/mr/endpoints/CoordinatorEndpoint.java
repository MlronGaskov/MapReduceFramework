package ru.nsu.mr.endpoints;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import ru.nsu.mr.config.Configuration;
import ru.nsu.mr.config.ConfigurationOption;
import ru.nsu.mr.endpoints.dto.JobQueueInfo;
import ru.nsu.mr.endpoints.dto.JobDetailInfo;
import ru.nsu.mr.endpoints.dto.NewJobDetails;
import ru.nsu.mr.endpoints.dto.TaskDetails;
import ru.nsu.mr.endpoints.dto.JobProgressInfo;
import ru.nsu.mr.gateway.HttpUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.Function;
import java.util.List;


public class CoordinatorEndpoint {
    private static final int STATUS_OK = 200;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;

    private final HttpServer httpServer;
    private final Consumer<String> onWorkerRegistration;
    private final Consumer<TaskDetails> onTaskNotification;
    private final Consumer<Configuration> onJobSubmission;
    private final Consumer<Integer> onDeleteJob;
    private final Supplier<List<JobQueueInfo>> onGetJobs;
    private final Function<Integer,JobDetailInfo> onGetJobDetails;
    private final Function<Integer,JobProgressInfo> onGetJobProgress;
    private final Supplier<Integer> onGetWorkerCount;

    public CoordinatorEndpoint(
            String coordinatorBaseUrl,
            Consumer<String> onWorkerRegistration,
            Consumer<TaskDetails> onTaskNotification,
            Consumer<Configuration> onJobSubmission,
            Supplier<List<JobQueueInfo>> onGetJobs,
            Function<Integer,JobDetailInfo> onGetJobDetails,
            Consumer<Integer> onDeleteJob,
            Function<Integer,JobProgressInfo> onGetJobProgress,
            Supplier<Integer> onGetWorkerCount) throws IOException {
        URI uri = URI.create(coordinatorBaseUrl);
        this.httpServer = HttpServer.create(new InetSocketAddress(uri.getHost(), uri.getPort()), 0);
        this.onWorkerRegistration = onWorkerRegistration;
        this.onTaskNotification = onTaskNotification;
        this.onJobSubmission = onJobSubmission;
        this.onGetJobs = onGetJobs;
        this.onGetJobDetails = onGetJobDetails;
        this.onDeleteJob = onDeleteJob;
        this.onGetJobProgress = onGetJobProgress;
        this.onGetWorkerCount = onGetWorkerCount;
        httpServer.createContext("/workers", new WorkerRegistrationHandler());
        httpServer.createContext("/workers/count", new WorkersCountHandler());
        httpServer.createContext("/notifyTask", new TaskNotificationHandler());
        httpServer.createContext("/job", new JobSubmissionHandler());
        httpServer.createContext("/jobs", new JobsQueryHandler());
    }

    public void startServer() {
        httpServer.start();
    }

    public void stopServer() {
        httpServer.stop(0);
    }

    private class WorkerRegistrationHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                HttpUtils.sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }
            try {
                String workerBaseUrl = HttpUtils.readRequestBody(exchange, String.class);
                onWorkerRegistration.accept(workerBaseUrl);
                HttpUtils.sendResponse(exchange, STATUS_OK, "Worker registered on url: " + workerBaseUrl);
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to register worker: " + e.getMessage());
            }
        }
    }

    private class WorkersCountHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                HttpUtils.sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }
            try {
                int count = onGetWorkerCount.get();
                HttpUtils.sendJsonResponse(exchange, STATUS_OK, count);
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to fetch worker count");
            }
        }
    }

    private class TaskNotificationHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                HttpUtils.sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }
            try {
                TaskDetails taskDetails = HttpUtils.readRequestBody(exchange, TaskDetails.class);
                onTaskNotification.accept(taskDetails);
                HttpUtils.sendResponse(exchange, STATUS_OK, "Task notification received for ID: " +
                        taskDetails.taskInformation().taskId());
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to process task notification: " + e.getMessage());
            }
        }
    }

    private class JobSubmissionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"PUT".equalsIgnoreCase(exchange.getRequestMethod())) {
                HttpUtils.sendErrorResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
                return;
            }
            try {
                NewJobDetails jobDetails = HttpUtils.readRequestBody(exchange, NewJobDetails.class);
                Configuration jobConfig = new Configuration()
                        .set(ConfigurationOption.JOB_ID, jobDetails.jobId())
                        .set(ConfigurationOption.JOB_PATH, jobDetails.jobPath())
                        .set(ConfigurationOption.JOB_STORAGE_CONNECTION_STRING, jobDetails.jobStorageConnectionString())
                        .set(ConfigurationOption.INPUTS_PATH, jobDetails.inputsPath())
                        .set(ConfigurationOption.MAPPERS_OUTPUTS_PATH, jobDetails.mappersOutputsPath())
                        .set(ConfigurationOption.REDUCERS_OUTPUTS_PATH, jobDetails.reducersOutputsPath())
                        .set(ConfigurationOption.DATA_STORAGE_CONNECTION_STRING, jobDetails.dataStorageConnectionString())
                        .set(ConfigurationOption.MAPPERS_COUNT, jobDetails.mappersCount())
                        .set(ConfigurationOption.REDUCERS_COUNT, jobDetails.reducersCount())
                        .set(ConfigurationOption.SORTER_IN_MEMORY_RECORDS, jobDetails.sorterInMemoryRecords());
                onJobSubmission.accept(jobConfig);
                HttpUtils.sendResponse(exchange, STATUS_OK, "Job accepted");
            } catch (IllegalStateException e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, e.getMessage());
            } catch (Exception e) {
                HttpUtils.sendErrorResponse(exchange, STATUS_BAD_REQUEST, "Failed to process job submission: " + e.getMessage());
            }
        }
    }

    private class JobsQueryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            String method = ex.getRequestMethod();
            String path   = ex.getRequestURI().getPath();
            String prefix = "/jobs";

            // GET /jobs
            if ("GET".equalsIgnoreCase(method) && prefix.equals(path)) {
                List<JobQueueInfo> list = onGetJobs.get();
                HttpUtils.sendJsonResponse(ex, STATUS_OK, list);

                // GET /jobs/{idx}/progress
            } else if ("GET".equalsIgnoreCase(method)
                    && path.startsWith(prefix + "/")
                    && path.endsWith("/progress")) {
                try {
                    String num = path.substring(
                            prefix.length() + 1,
                            path.length() - "/progress".length()
                    );
                    int idx = Integer.parseInt(num);
                    JobProgressInfo prog = onGetJobProgress.apply(idx);
                    HttpUtils.sendJsonResponse(ex, STATUS_OK, prog);
                } catch (NumberFormatException | IndexOutOfBoundsException | IllegalStateException e) {
                    HttpUtils.sendErrorResponse(ex, STATUS_BAD_REQUEST, "Invalid job index");
                }

                // GET /jobs/{idx}
            } else if ("GET".equalsIgnoreCase(method) && path.startsWith(prefix + "/")) {
                try {
                    int idx = Integer.parseInt(path.substring(prefix.length() + 1));
                    JobDetailInfo detail = onGetJobDetails.apply(idx);
                    HttpUtils.sendJsonResponse(ex, STATUS_OK, detail);
                } catch (Exception e) {
                    HttpUtils.sendErrorResponse(ex, STATUS_BAD_REQUEST, "Invalid job index");
                }

                // DELETE /jobs/{idx}
            } else if ("DELETE".equalsIgnoreCase(method) && path.startsWith(prefix + "/")) {
                try {
                    int idx = Integer.parseInt(path.substring(prefix.length() + 1));
                    onDeleteJob.accept(idx);
                    HttpUtils.sendResponse(ex, STATUS_OK, "Job " + idx + " deleted");
                } catch (IndexOutOfBoundsException e) {
                    HttpUtils.sendErrorResponse(ex, STATUS_BAD_REQUEST, "Invalid job index");
                } catch (Exception e) {
                    HttpUtils.sendErrorResponse(ex, STATUS_BAD_REQUEST, "Failed to delete job");
                }

            } else {
                HttpUtils.sendErrorResponse(ex, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed");
            }
        }
    }

}
