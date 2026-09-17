package org.example;
import com.google.common.util.concurrent.AtomicDouble;
import java.util.function.DoubleUnaryOperator;

public class Main {
    public static void main(String[] args) {
        DoubleUnaryOperator function = x -> Math.sin(x);
        double a = 0.0;
        double b = Math.PI;
        int n = 10_000_000;
        int nThreads = 4;
        calculateLeftRectangleIntegral(function, a, b, n);
        calculateLeftRectangleIntegralParallel(function, a, b, n,nThreads);
        calculateLeftRectangleIntegralAtomic(function, a, b, n,nThreads);
        calculateLeftRectangleIntegralMonitor(function, a, b, n,nThreads);

    }

    static class Res{
        volatile double sum = 0;
        synchronized public void add(double a){
            sum += a;
        }
        synchronized public double get(){
            return sum;
        }
    }

    public static double calculateLeftRectangleIntegral(DoubleUnaryOperator f, double a, double b, int n) {
        long startTime = System.nanoTime();
        double h = (b - a) / n;
        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            double x = a + i * h;
            sum += f.applyAsDouble(x);
        }
        double integralResult = sum * h;
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.println("Итоговое значение интеграла: " + integralResult);
        System.out.println("Время вычисления: " + durationMs + " мс");
        return integralResult;
    }

    public static double calculateLeftRectangleIntegralParallel(
            DoubleUnaryOperator f, double a, double b, int n, int THREADS) {
        double h = (b - a) / n;
        Res res = new Res();
        int stepsPerThread = n / THREADS;
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];
        long startTime = System.nanoTime();
        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * stepsPerThread;
            final int startStep = threadsStart[i];
            final int endStep = (i == THREADS - 1) ? n : startStep + stepsPerThread;
            threads[i] = new Thread(() -> {
                double localSum = 0.0;
                for (int j = startStep; j < endStep; j++) {
                    double x = a + j * h;
                    localSum += f.applyAsDouble(x);
                }
                res.add(localSum);

            });

            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        double integralResult = res.get() * h;
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.println("Итоговое значение интеграла: " + integralResult);
        System.out.println("Время параллельного вычисления: " + durationMs + " мс");

        return integralResult;
    }

    public static double calculateLeftRectangleIntegralAtomic(
            DoubleUnaryOperator f, double a, double b, int n, int THREADS) {
        AtomicDouble totalSum = new AtomicDouble(0.0);
        double h = (b - a) / n;
        int stepsPerThread = n / THREADS;
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];
        long startTime = System.nanoTime();
        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * stepsPerThread;
            final int startStep = threadsStart[i];
            final int endStep = (i == THREADS - 1) ? n : startStep + stepsPerThread;
            threads[i] = new Thread(() -> {
                double localSum = 0.0;
                for (int j = startStep; j < endStep; j++) {
                    double x = a + j * h;
                    localSum += f.applyAsDouble(x);
                }
                totalSum.addAndGet(localSum);
            });

            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        double integralResult = totalSum.get() * h;
        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.println("Итоговое значение интеграла: " + integralResult);
        System.out.println("Время вычисления (AtomicDouble): " + durationMs + " мс");
        return integralResult;
    }

    public static double calculateLeftRectangleIntegralMonitor(
            DoubleUnaryOperator f, double a, double b, int n, int THREADS) {
        Res totalSum = new Res();
        final Object monitor = new Object();
        long startTime = System.nanoTime();
        double h = (b - a) / n;
        int stepsPerThread = n / THREADS;
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];

        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * stepsPerThread;
            final int startStep = threadsStart[i];
            final int endStep = (i == THREADS - 1) ? n : startStep + stepsPerThread;

            threads[i] = new Thread(() -> {
                double localSum = 0.0;
                for (int j = startStep; j < endStep; j++) {
                    double x = a + j * h;
                    localSum += f.applyAsDouble(x);
                }
                synchronized (monitor) {
                    totalSum.add(localSum);
                }
            });

            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        double integralResult = totalSum.get() * h;

        long endTime = System.nanoTime();
        double durationMs = (endTime - startTime) / 1_000_000.0;
        System.out.println("Итоговое значение интеграла: " + integralResult);
        System.out.println("Время вычисления (монитор): " + durationMs + " мс");

        return integralResult;
    }
}