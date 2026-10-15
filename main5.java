import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class LogAnalysisAssignment {

    public record LogEntry(String method, String path, int status, long responseBytes, long durationMillis) {}
    public record LogFile(Path path) {}
    public record PathStatistics(long requests, long successful, long errors, long responseBytes, long totalDurationMillis, long maxDurationMillis) {}
    public record Report(long totalRequests, long successfulRequests, long errorRequests, long responseBytes, long maxDurationMillis, int failedFilesCount, long corruptedLinesCount) {}

    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) { close(); }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException("Cannot read " + path, exception);
                        }
                    }

                    private void close() {
                        if (closed) return;
                        closed = true;
                        try { reader.close(); }
                        catch (IOException exception) { throw new RuntimeException("Cannot close " + path, exception); }
                    }

                    @Override public boolean hasNext() { return nextLine != null; }

                    @Override public String next() {
                        if (nextLine == null) throw new NoSuchElementException();
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException("Cannot open " + path, exception);
            }
        }
    }

    public static final class GenerateLogs {
        private static final String[] METHODS = {"GET", "POST", "PUT", "DELETE"};
        private static final String[] PATHS = {"/", "/index.html", "/login", "/api/users", "/api/orders", "/api/products", "/search", "/health"};
        private static final int[] STATUSES = {200, 200, 200, 201, 204, 301, 400, 401, 404, 500};
        private GenerateLogs() {}

        public static void generate(Path outputDirectory, int fileCount, int linesPerFile) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) throw new IllegalArgumentException("Counts must be non-negative");
            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(String.format("access-%03d.log", fileId));
                try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                    for (int line = 0; line < linesPerFile; line++) {
                        if (line == 7 && fileId == 2) {
                            writer.write("CORRUPTED LINE MISSING FIELDS\n");
                            continue;
                        }
                        String method = METHODS[random.nextInt(METHODS.length)];
                        String path = PATHS[random.nextInt(PATHS.length)];
                        int status = STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes = 100L + random.nextInt(100_000);
                        long durationMillis = 1L + random.nextInt(500);
                        writer.write("%s %s %d %d %d%n".formatted(method, path, status, responseBytes, durationMillis));
                    }
                }
            }
        }
    }

    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);
        int files = 0, lines = 0;

        try (Stream<Path> paths = Files.list(directory)) {
            for (Path path : paths.collect(Collectors.toList())) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (line.startsWith("CORRUPTED")) continue;
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) throw new AssertionError("Unexpected line: " + line);
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }
        System.out.println("OK: generated " + files + " files and read valid lines successfully.");
    }

    private static class PathStatsAccumulator {
        long requests = 0, successful = 0, errors = 0, responseBytes = 0, totalDurationMillis = 0, maxDurationMillis = 0;

        public synchronized void add(LogEntry entry) {
            requests++;
            if (entry.status() >= 200 && entry.status() <= 399) successful++;
            else errors++;
            responseBytes += entry.responseBytes();
            totalDurationMillis += entry.durationMillis();
            if (entry.durationMillis() > maxDurationMillis) {
                maxDurationMillis = entry.durationMillis();
            }
        }

        public PathStatistics toSnapshot() {
            return new PathStatistics(requests, successful, errors, responseBytes, totalDurationMillis, maxDurationMillis);
        }
    }

    private static class StopFlag {
        volatile boolean isStopped = false;
    }

    public static Report analyze(Path directory, int threadCount, int semaphoreLimit) throws InterruptedException {
        System.out.println("Начинаю анализ: потоков=" + threadCount + ", лимит семафора=" + semaphoreLimit);

        AtomicLong totalLinesRead = new AtomicLong(0);
        AtomicLong validRecords = new AtomicLong(0);
        AtomicLong corruptedRecords = new AtomicLong(0);
        AtomicInteger activeFiles = new AtomicInteger(0);
        AtomicInteger maxActiveFiles = new AtomicInteger(0);
        AtomicInteger processedFiles = new AtomicInteger(0);
        AtomicInteger failedFiles = new AtomicInteger(0);

        StopFlag stopFlag = new StopFlag();
        Semaphore semaphore = new Semaphore(semaphoreLimit);
        ConcurrentHashMap<String, PathStatsAccumulator> pathStats = new ConcurrentHashMap<>();

        java.util.List<Path> filesToProcess;
        try (Stream<Path> stream = Files.list(directory)) {
            filesToProcess = stream.filter(p -> p.toString().endsWith(".log")).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Не удалось прочитать каталог " + directory, e);
        }

        CountDownLatch latch = new CountDownLatch(filesToProcess.size());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (Path file : filesToProcess) {
            executor.submit(() -> {
                try {
                    semaphore.acquire();
                    try {
                        int currentActive = activeFiles.incrementAndGet();

                        int currentMax;
                        do {
                            currentMax = maxActiveFiles.get();
                            if (currentActive <= currentMax) break;
                        } while (!maxActiveFiles.compareAndSet(currentMax, currentActive));

                        if (stopFlag.isStopped) return;

                        Iterator<String> iterator = LogFileLines.iterator(file);

                        while (iterator.hasNext() && !stopFlag.isStopped) {
                            String line = iterator.next();
                            totalLinesRead.incrementAndGet();
                            if (line == null || line.trim().isEmpty()) continue;

                            try {
                                String[] parts = line.trim().split("\\s+");
                                if (parts.length != 5) throw new IllegalArgumentException("Неверное кол-во полей: " + line);

                                LogEntry entry = new LogEntry(
                                    parts[0], parts[1],
                                    Integer.parseInt(parts[2]),
                                    Long.parseLong(parts[3]),
                                    Long.parseLong(parts[4])
                                );
                                validRecords.incrementAndGet();

                                pathStats.computeIfAbsent(entry.path(), k -> new PathStatsAccumulator()).add(entry);

                            } catch (IllegalArgumentException e) {
                                corruptedRecords.incrementAndGet();
                            }
                        }
                        processedFiles.incrementAndGet();
                    } finally {
                        activeFiles.decrementAndGet();
                        semaphore.release();
                        latch.countDown();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failedFiles.incrementAndGet();
                    latch.countDown();
                } catch (Exception e) {
                    System.err.println("Ошибка обработки файла " + file + ": " + e.getMessage());
                    failedFiles.incrementAndGet();
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        long totalRequests = 0, successfulRequests = 0, errorRequests = 0, totalResponseBytes = 0, globalMaxDuration = 0;
        for (PathStatsAccumulator acc : pathStats.values()) {
            PathStatistics stats = acc.toSnapshot();
            totalRequests += stats.requests();
            successfulRequests += stats.successful();
            errorRequests += stats.errors();
            totalResponseBytes += stats.responseBytes();
            if (stats.maxDurationMillis() > globalMaxDuration) {
                globalMaxDuration = stats.maxDurationMillis();
            }
        }

        System.out.println("\n--- Статистика выполнения ---");
        System.out.println("Прочитано строк: " + totalLinesRead.get());
        System.out.println("Корректных записей: " + validRecords.get());
        System.out.println("Повреждённых записей: " + corruptedRecords.get());
        System.out.println("Обработано файлов: " + processedFiles.get());
        System.out.println("Ошибок файлов: " + failedFiles.get());
        System.out.println("Макс. одновременных файлов: " + maxActiveFiles.get() + " (лимит: " + semaphoreLimit + ")");
        System.out.println("Активных файлов в конце: " + activeFiles.get() + " (должно быть 0)");

        return new Report(totalRequests, successfulRequests, errorRequests, totalResponseBytes, globalMaxDuration, failedFiles.get(), corruptedRecords.get());
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs");
            int files = args.length > 2 ? Integer.parseInt(args[2]) : 20;
            int lines = args.length > 3 ? Integer.parseInt(args[3]) : 1_000;
            GenerateLogs.generate(dir, files, lines);
            System.out.println("Generated " + files + " files in " + dir);
            return;
        }
        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }
        if (args.length > 0 && args[0].equals("analyze")) {
            Path dir = Path.of(args.length > 1 ? args[1] : "logs");
            int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
            int semLimit = args.length > 3 ? Integer.parseInt(args[3]) : 4;
            Report report = analyze(dir, threads, semLimit);
            System.out.println("\nИтоговый Report:");
            System.out.println("  totalRequests: " + report.totalRequests());
            System.out.println("  successfulRequests: " + report.successfulRequests());
            System.out.println("  errorRequests: " + report.errorRequests());
            System.out.println("  Инвариант (requests = successful + errors): " +
                (report.totalRequests() == report.successfulRequests() + report.errorRequests() ? "PASS" : "FAIL"));
            return;
        }
        System.out.println("Usage:");
        System.out.println("  java main3.java generate <directory> <files> <lines>");
        System.out.println("  java main3.java check");
        System.out.println("  java main3.java analyze <directory> <threads> <semaphoreLimit>");
    }
}
