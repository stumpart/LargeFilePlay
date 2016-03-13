package concurrency.fileprocessing;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import java.nio.charset.StandardCharsets;

public class InjestionWorker implements Runnable{
    private final ArrayBlockingQueue queue;
    private final ScheduledExecutorService consumerScheduler;
    private final CountDownLatch countDownLatch;
    private final Path filePath;

    public InjestionWorker(ArrayBlockingQueue q,
                           ScheduledExecutorService consumerScheduler,
                           CountDownLatch countDownLatch,
                           Path filePath){
        this.queue = q;
        this.consumerScheduler = consumerScheduler;
        this.countDownLatch = countDownLatch;
        this.filePath = filePath;
    }

    @Override
    public void run(){
        try{
            try(Stream<String> lines = Files.lines(filePath, StandardCharsets.UTF_8)){
                lines.onClose(() -> {
                    while(!queue.isEmpty()){
                        //Waiting on queue to be empty
                    }
                    System.out.println("Done .....");
                    countDownLatch.countDown();
                    consumerScheduler.shutdownNow();
                }).forEach((l) -> {
                            try {
                                queue.put(l);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        });
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}