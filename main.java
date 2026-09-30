import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class WorkStealing {
    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    interface Shutdownable {
        void shutdown();
    }

    public interface ShutdownableExecutor extends Shutdownable, Executor {}

    static class ThreadPerTaskExecutor implements ShutdownableExecutor {
        List<Thread> threads = new ArrayList<>();

        @Override
        public void execute(Runnable command) {
            var t = new Thread(command);
            threads.add(t);
            t.start();
        }

        public void shutdown() {
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {
        protected static final Runnable EXIT_TASK = () -> {};
        AtomicInteger counter = new AtomicInteger(0);
        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threads) {
            Supplier<IntStream> stream = () -> IntStream.iterate(0, x -> x < threads, x -> x + 1);
            tasks = new Vector<>(
                    stream.get()
                            .mapToObj(x -> new LinkedBlockingDeque<Runnable>())
                            .toList());
            this.threads = stream.get()
                    .mapToObj(this::spawnThread)
                    .toList();
            this.threads.forEach(Thread::start);
        }

        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task;
                    try {
                        task = tasks.get(id).takeFirst();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (task == EXIT_TASK)
                        return;
                    task.run();
                }
            });
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown)
                throw new RejectedExecutionException("Executor is shutting down");
            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {
        WorkStealingExecutor(int threads) {
            super(threads);
        }

        @Override
        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();
                    if (task == null)
                        task = stealTask(id);
                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == null)
                            continue;
                    }
                    if (task == EXIT_TASK) {
                        task = stealTask(id);
                        if (task == null)
                            return;
                        tasks.get(id).addLast(EXIT_TASK);
                    }
                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();
                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }
                if (task != null)
                    return task;
            }
            return null;
        }
    }

    static class FixedThreadPoolExecutor implements ShutdownableExecutor {
        private final ExecutorService delegate;

        FixedThreadPoolExecutor(int threads) {
            delegate = Executors.newFixedThreadPool(threads);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
            try {
                if (!delegate.awaitTermination(60, TimeUnit.SECONDS)) {
                    delegate.shutdownNow();
                }
            } catch (InterruptedException e) {
                delegate.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000;
    static final int MEAN_TASK_TIME = (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER);
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2 + 1;
    static final int ESTIMATED_OPTIMAL_TIME = MEAN_TASK_TIME * TASK_NUMBER / THREAD_NUMBER;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;


    static long BASE_DIFFICULTY = 1_000_000;

    static void calibrateBlackHole() {
        long testIter = 10_000_000;
        long start = System.nanoTime();
        blackHole(testIter);
        long end = System.nanoTime();
        double nsPerIter = (end - start) / (double) testIter;
        long targetNs = MEAN_TASK_TIME * 1_000_000L;
        BASE_DIFFICULTY = Math.max(1, (long) (targetNs / nsPerIter));
        System.out.println("Calibrated BASE_DIFFICULTY: " + BASE_DIFFICULTY + " iterations per mean task");
    }
    static long blackHole(long difficulty) {
        long sum = 0;
        for (long i = 0; i < difficulty; i++) {
            sum += i;
            if (sum == Long.MIN_VALUE) break;
        }
        return sum;
    }
    static Runnable createTask(long weight) {
        long difficulty = (weight * BASE_DIFFICULTY) / MEAN_TASK_TIME;
        return () -> {
            long res = blackHole(difficulty);
            if (res == Long.MIN_VALUE) {
                System.out.println("Unreachable");
            }
        };
    }

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);
            case PERIODIC -> taskId % THREAD_NUMBER == 0
                    ? MEAN_TASK_TIME * THREAD_NUMBER
                    : 0;
            case PARETO -> {
                var scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                var duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
            }
        };
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        var durations = new int[TASK_NUMBER];
        long totalDuration = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }
        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }
        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }
        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }
        return durations;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution) {
        var weights = createTaskDurations(distribution);
        return new Vector<>(
                IntStream
                        .iterate(0, x -> x < TASK_NUMBER, x -> x + 1)
                        .mapToObj(x -> createTask(weights[x]))
                        .toList()
        );
    }

    public record Pair<A, B>(A first, B second) {};

    public static Pair<Long, Long> measureExecutor(ShutdownableExecutor executor, TaskDistribution distribution) {
        var tasks = createTasks(distribution);
        var start = System.nanoTime();
        tasks.forEach(executor::execute);
        var submitionEnd = System.nanoTime();
        executor.shutdown();
        var finish = System.nanoTime();
        return new Pair<>(submitionEnd - start, finish - start);
    }

    public static void main(String[] args) {
        calibrateBlackHole();

        System.out.println("Target optimal time: " + TARGET_OPTIMAL_FULL_TIME);
        System.out.println("Estimated optimal time: " + ESTIMATED_OPTIMAL_TIME);

        var threadPerTaskExecutorResult = measureExecutor(new ThreadPerTaskExecutor(), TaskDistribution.UNIFORM);
        System.out.println("Thread per task executor submtition time: " + threadPerTaskExecutorResult.first / 1_000_000d);
        System.out.println("Thread per task execution time: " + threadPerTaskExecutorResult.second / 1_000_000d);

        for (var distribution : TaskDistribution.values()) {
            System.out.println("\nTask distribution: " + distribution);

            var roundRobinExecutorResult = measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution);
            System.out.println("Round Robin Executor submition time: " + roundRobinExecutorResult.first / 1_000_000d);
            System.out.println("Round Robin Executor execution time: " + roundRobinExecutorResult.second / 1_000_000d);

            var workStealingExecutorResult = measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution);
            System.out.println("Work Stealing Executor submition time: " + workStealingExecutorResult.first / 1_000_000d);
            System.out.println("Work Stealing Executor execution time: " + workStealingExecutorResult.second / 1_000_000d);

            var fixedThreadPoolResult = measureExecutor(new FixedThreadPoolExecutor(THREAD_NUMBER), distribution);
            System.out.println("Fixed ThreadPool Executor submition time: " + fixedThreadPoolResult.first / 1_000_000d);
            System.out.println("Fixed ThreadPool Executor execution time: " + fixedThreadPoolResult.second / 1_000_000d);
        }

        /*
         *
         * ThreadPerTaskExecutor:
         *    На 100_000 задачах создаст 100к потоков. Это приведёт к огромным затратам памяти
         *    и, скорее всего, к OutOfMemoryError
         *    или дикой деградации из-за переключений контекста.
         *    Время работы будет огромным из-за создания потоков.
         *
         * RoundRobinExecutor:
         *    UNIFORM: Отлично, равномерно распределяет нагрузку.
         *    PERIODIC/PARETO: Провал. Один поток получит тяжёлую задачу и будет её долго считать,
         *    а остальные потоки, закончив свои лёгкие задачи, будут простаивать в ожидании новых
         *    по круговому циклу. Общее время выполнения сильно вырастет.
         *
         * WorkStealingExecutor:
         *    UNIFORM: Чуть медленнее RoundRobin из-за оверхеда на проверку воровства задачи.
         *    PERIODIC/PARETO: Потоки, которые освободились, берут задачи из хвоста
         *    очередей других потоков. Это идеально балансирует нагрузку при неравномерном распределении.
         *
         * FixedThreadPoolExecutor:
         *    Использует единую очередь.
         *    UNIFORM/PARETO: Работает хорошо, но может уступить WorkStealing из-за
         *    блокировок на единой очереди при подаче 100к задач. Submission time может быть выше.
         *    PERIODIC: Лучше RoundRobin, так как свободные потоки сразу берут следующие задачи
         *    из общей очереди, но всё же может проиграть WorkStealing из-за локов на очереди.
         *
         * Sleep vs BlackHole:
         *    На Thread.sleep() потоки просто спят. Переключений контекста мало, очереди не являются
         *    узким местом, так как задачи подаются быстро, а выполняются долго.
         *    На blackHole() потоки активно грузят CPU. Единая очередь FixedThreadPool становится
         *    узким местом.
         *    WorkStealing с пер-потоковыми очередями покажет себя лучше, так как подача задач
         *    идёт в локальную очередь без глобальных блокировок, а балансировка происходит динамически.
         *
         */
    }
}
