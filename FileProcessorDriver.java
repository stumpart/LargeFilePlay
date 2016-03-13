package concurrency.fileprocessing;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class FileProcessorDriver {

    private static final int SCHEDULERS_COUNT = 30;
    private static final int QUEUE_TOTAL = 30;

    public static void main(String[] args) throws InterruptedException {
        Instant startTime  = Instant.now();
        ArrayBlockingQueue<String> ab = new ArrayBlockingQueue<>(QUEUE_TOTAL);
        ScheduledExecutorService e2 = Executors.newScheduledThreadPool(QUEUE_TOTAL, new WorkerThreadFactory());
        ExecutorService e1 = Executors.newFixedThreadPool(1, new WorkerThreadFactory());
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        if(args.length > 0){
            Path path = Paths.get(args[0]);
            e1.submit(new InjestionWorker(ab, e2, countDownLatch, path));
            Consumer<String> consumer = (l) -> {}; //Empty lambda. Put logic here for processing the lines

            for(int i = 0; i < SCHEDULERS_COUNT; i++){
                e2.scheduleAtFixedRate(new FileLineWorker(ab, counter, consumer), 1, 1, TimeUnit.MILLISECONDS);
            }

            countDownLatch.await();
            e1.shutdown();
            e2.shutdown();
            memoryPools();
            Instant endTime = Instant.now();
            Duration duration = Duration.between(startTime, endTime);
            System.out.println("Total Number of Records :" + counter.get());
            outDuration(duration);
        }else{
           System.out.println("No File provided");
        }
    }

    public static void memoryPools(){
        String systemUsage = "";
        List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
        double sysLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();

        systemUsage += String.format("System Load : %s%n", sysLoad);
        systemUsage += String.format("Peak Thread Count : %d%n", ManagementFactory.getThreadMXBean().getPeakThreadCount());

        for (MemoryPoolMXBean pool : pools) {
            MemoryUsage peak = pool.getPeakUsage();
            systemUsage += String.format("Peak %s memory used: %s%n", pool.getName(),printUnitFromBytes(peak.getUsed()));
            systemUsage += String.format("Peak %s memory committed: %s%n", pool.getName(), printUnitFromBytes(peak.getCommitted()));
        }

        System.out.println(systemUsage);
    }

    public static void outDuration(Duration duration){
        System.out.println("Total running time :" + duration.toMillis());
    }

    /**
     * Coming from https://github.com/apache/camel/blob/master/camel-core/src/main/java/org/apache/camel/util/UnitUtils.java
     * @param bytes
     * @return
     */
    public static String printUnitFromBytes(long bytes) {
        int unit = 1000;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = "" + "KMGTPE".charAt(exp - 1);
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}