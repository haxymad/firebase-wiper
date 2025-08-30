import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FirebaseWiper {

    private final String baseUrl;
    private final int maxWorkers;
    private final HttpClient httpClient;
    private final ThreadPoolExecutor executor;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final AtomicInteger deletedCount = new AtomicInteger(0);
    private final AtomicInteger failedCount = new AtomicInteger(0);

    public FirebaseWiper(String baseUrl, int maxWorkers) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.maxWorkers = maxWorkers;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxWorkers);
    }

    private String buildUrl(String path, boolean shallow) {
        String fullPath = path.isEmpty() ? "" : "/" + path;
        String url = this.baseUrl + fullPath + ".json";
        if (shallow) {
            url += "?shallow=true";
        }
        return url;
    }

    private void deletePath(String path) {
        activeTasks.incrementAndGet();
        executor.submit(() -> {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(buildUrl(path, false)))
                        .timeout(Duration.ofSeconds(30))
                        .DELETE()
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    deletedCount.incrementAndGet();
                } else if (response.body() != null && response.body().contains("exceeds the maximum size")) {
                    // Too large, so we need to drill down.
                    drillDown(path);
                } else {
                    System.err.println("Failed to delete " + path + ": " + response.statusCode() + " " + response.body());
                    failedCount.incrementAndGet();
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("Exception on path " + path + ": " + e.getMessage());
                failedCount.incrementAndGet();
            } finally {
                activeTasks.decrementAndGet();
            }
        });
    }

    private void drillDown(String path) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(buildUrl(path, true)))
                    .timeout(Duration.ofSeconds(20))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                // Simple regex to find keys in the shallow JSON response
                Pattern p = Pattern.compile("\"(.*?)\":true");
                Matcher m = p.matcher(response.body());
                while (m.find()) {
                    String key = m.group(1);
                    String childPath = path.isEmpty() ? key : path + "/" + key;
                    deletePath(childPath);
                }
            } else {
                System.err.println("Failed to get shallow keys for " + path + ": " + response.statusCode());
                failedCount.incrementAndGet();
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Exception on shallow request " + path + ": " + e.getMessage());
            failedCount.incrementAndGet();
        }
    }

    public void run() throws InterruptedException {
        System.out.println("--- Starting Firebase RTDB Recursive Wiper (Java) ---");
        System.out.println("Target: " + this.baseUrl);
        
        drillDown(""); // Start the process from the root

        // Monitor progress
        while (true) {
            int active = activeTasks.get();
            long completed = executor.getCompletedTaskCount();
            long total = executor.getTaskCount();
            
            System.out.printf(
                "[PROGRESS] Deleted: %d | Failed: %d | Active Threads: %d | Queue Size: %d%n",
                deletedCount.get(),
                failedCount.get(),
                active,
                executor.getQueue().size()
            );

            if (active == 0 && executor.getQueue().isEmpty()) {
                break;
            }
            Thread.sleep(2000);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        
        System.out.println("\n--- Wipe complete ---");
        System.out.printf("Final Stats -> Deleted: %d | Failed: %d%n", deletedCount.get(), failedCount.get());
    }

    public static void main(String[] args) {
        String baseUrl = null;
        int maxWorkers = 50; // Default workers

        for (int i = 0; i < args.length; i++) {
            if ("--base-url".equals(args[i]) && i + 1 < args.length) {
                baseUrl = args[i + 1];
            }
            if ("--max-workers".equals(args[i]) && i + 1 < args.length) {
                try {
                    maxWorkers = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number for --max-workers");
                    System.exit(1);
                }
            }
        }

        if (baseUrl == null) {
            System.err.println("Usage: java FirebaseWiper --base-url <your-firebase-url> [--max-workers <number>]");
            System.exit(1);
        }

        try {
            FirebaseWiper wiper = new FirebaseWiper(baseUrl, maxWorkers);
            wiper.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Wipe process was interrupted.");
        }
    }
}
