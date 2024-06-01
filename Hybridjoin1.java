package proj;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import proj.StreamDataGenerator.SystemController;

public class Hybridjoin1 {
    public static <ProductInfo> void main(String[] args) {
        // Initialize data structures
        BlockingQueue<SalesRecord> streamBuffer = new LinkedBlockingQueue<>();
        HashMap<Integer, SalesRecord> multiHashTable = new HashMap<>();
        BlockingQueue<Integer> queue = new LinkedBlockingQueue<>();
        HashMap<Integer, proj.ProductInfo> diskBuffer = new HashMap<>();

        // Create instances of classes
        DataGenerator streamGenerator = new DataGenerator(streamBuffer);
        JoinProcessor hybridJoin = new JoinProcessor(streamBuffer, multiHashTable, queue, diskBuffer);
       // MainController controller = new MainController(streamGenerator, hybridJoin);

        // Start the threads
        streamGenerator.start();
        hybridJoin.start();
        //controller.start();
    }
}