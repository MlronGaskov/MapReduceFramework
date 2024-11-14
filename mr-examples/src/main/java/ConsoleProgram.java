import static java.lang.Thread.sleep;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class ConsoleProgram {
    private record TaskDto(int taskId, int progress) {}

    private static class Worker {

        private static class Task {
            public int taskId;
            public int progress = 0;

            public Task(int taskId) {
                this.taskId = taskId;
            }

            public TaskDto toDto() {
                return new TaskDto(taskId, progress);
            }

            public void progress() {
                progress += 10;
            }
        }

        Map<Integer, Task> prevTasks = new HashMap<>();
        private Task task;

        public Worker() {
            task = null;
        }

        public void putTask(int taskId) {
            if (this.task != null) {
                System.out.println("task is not added");
                return;
            }
            System.out.println("task is added");
            this.task = new Task(taskId);
        }

        public void waitTask() throws InterruptedException {
            while (task == null) {
                sleep(1000);
            }
            System.out.println("received task: " + task.taskId);
        }

        public List<TaskDto> tasks() {
            List<TaskDto> tasks =
                    new java.util.ArrayList<>(
                            prevTasks.values().stream().map(Task::toDto).toList());
            if (task != null) {
                tasks.add(task.toDto());
            }
            return tasks;
        }

        public void doTask() throws InterruptedException {
            for (int i = 0; i < 10; ++i) {
                sleep(1000);
                task.progress();
                System.out.println(task.progress);
            }
            System.out.println("task done " + task.taskId);
            prevTasks.put(task.taskId, task);
            task = null;
        }

        public TaskDto getTaskDto(int taskId) throws Exception {
            if (task != null && task.taskId == taskId) {
                return task.toDto();
            } else if (prevTasks.containsKey(taskId)) {
                return prevTasks.get(taskId).toDto();
            }
            throw new Exception("no such task");
        }

        public void run() throws InterruptedException {
            while (true) {
                waitTask();
                doTask();
            }
        }
    }

    public static void main(String[] args) {
        Worker worker = new Worker();

        Thread consoleThread =
                new Thread(
                        () -> {
                            Scanner scanner = new Scanner(System.in);
                            while (true) {
                                String input = scanner.nextLine();
                                if (input.charAt(0) == 'p') {
                                    worker.putTask(Integer.parseInt(input.substring(2)));
                                    System.out.println("Task added to worker.");
                                }
                                if (input.charAt(0) == 's') {
                                    try {
                                        System.out.println(
                                                worker.getTaskDto(
                                                        Integer.parseInt(input.substring(2))));
                                    } catch (Exception e) {
                                        System.out.println("no such task");
                                    }
                                }
                                if (input.charAt(0) == 'l') {
                                    System.out.println(worker.tasks());
                                }
                            }
                        });

        consoleThread.start();

        try {
            worker.run();
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }
    }
}
