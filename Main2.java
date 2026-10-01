import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import javax.imageio.ImageIO;

public class MandelbrotParallel {
    private static final int WIDTH = 1920;
    private static final int HEIGHT = 1080;
    private static final int MAX_ITER = 500;
    private static final double X_MIN = -2.5;
    private static final double X_MAX = 1.0;
    private static final double Y_MIN = -1.25;
    private static final double Y_MAX = 1.25;
    static class RowTask implements Callable<int[]> {
        private final int row;
        private final int width;
        private final int maxIter;
        private final double xMin, xMax, yMin, yMax;

        RowTask(int row, int width, int maxIter,
                double xMin, double xMax, double yMin, double yMax) {
            this.row = row;
            this.width = width;
            this.maxIter = maxIter;
            this.xMin = xMin;
            this.xMax = xMax;
            this.yMin = yMin;
            this.yMax = yMax;
        }
        public int[] call() {
            int[] iterations = new int[width];
            double y = yMax - (row / (double) (height() - 1)) * (yMax - yMin);

            for (int x = 0; x < width; x++) {
                double cx = xMin + (x / (double) (width - 1)) * (xMax - xMin);
                iterations[x] = mandelbrot(cx, y, maxIter);
            }
            return iterations;
        }
        private int height() {
            return HEIGHT;
        }
        private int mandelbrot(double cx, double cy, int maxIter) {
            double zx = 0.0, zy = 0.0;
            double zx2 = 0.0, zy2 = 0.0;
            int iter = 0;
            while (zx2 + zy2 <= 4.0 && iter < maxIter) {
                zy = 2.0 * zx * zy + cy;
                zx = zx2 - zy2 + cx;
                zx2 = zx * zx;
                zy2 = zy * zy;
                iter++;
            }
            return iter;
        }
    }
    private static int colorForIteration(int iter, int maxIter) {
        if (iter == maxIter) {
            return 0xFF000000;
        }
        float hue = (iter % 256) / 256.0f;
        float saturation = 0.85f;
        float brightness = (iter < maxIter) ? 1.0f : 0.0f;
        int rgb = java.awt.Color.HSBtoRGB(hue, saturation, brightness);
        return rgb;
    }

    public static void main(String[] args) throws Exception {
        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<int[]>> futures = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        for (int row = 0; row < HEIGHT; row++) {
            futures.add(executor.submit(
                new RowTask(row, WIDTH, MAX_ITER, X_MIN, X_MAX, Y_MIN, Y_MAX)
            ));
        }
        int[][] results = new int[HEIGHT][WIDTH];
        for (int row = 0; row < HEIGHT; row++) {
            results[row] = futures.get(row).get();
        }
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        for (int y = 0; y < HEIGHT; y++) {
            for (int x = 0; x < WIDTH; x++) {
                image.setRGB(x, y, colorForIteration(results[y][x], MAX_ITER));
            }
        }
        String filename = "mandelbrot.png";
        ImageIO.write(image, "png", new File(filename));
        executor.shutdown();
    }
}
