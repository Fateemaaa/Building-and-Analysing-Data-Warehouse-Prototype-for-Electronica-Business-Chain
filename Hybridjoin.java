package proj;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class SalesRecord {
    private int customerId;
    private int productId;
    private int timeId;
    private int storeId;
    private int supplierId;
    private int orderId;
    private int customerID;
    private int productID;
    private String customerName;
    private String gender;
    private int quantityOrdered;

    public SalesRecord(int customerId, int productId, int timeId, int storeId, int supplierId) {
        this.customerId = customerId;
        this.productId = productId;
        this.timeId = timeId;
        this.storeId = storeId;
        this.supplierId = supplierId;
    }

    public int getOrderId() {
        return orderId;
    }

    public int getCustomerID() {
        return customerID;
    }

    public int getProductID() {
        return productID;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getGender() {
        return gender;
    }

    public int getQuantityOrdered() {
        return quantityOrdered;
    }
}

class ProductInfo {
    private int productId;
    private String productName;
    private double productPrice;
    private int supplierId;
    private String supplierName;
    private int storeId;
    private String storeName;

    public ProductInfo(int productId, String productName, double productPrice, int supplierId, String supplierName, int storeId, String storeName) {
        this.productId = productId;
        this.productName = productName;
        this.productPrice = productPrice;
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.storeId = storeId;
        this.storeName = storeName;
    }

    public int getProductID() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public double getProductPrice() {
        return productPrice;
    }

    public int getSupplierId() {
        return supplierId;
    }

    public String getSupplierName() {
        return supplierName;
    }

    public int getStoreId() {
        return storeId;
    }

    public String getStoreName() {
        return storeName;
    }
}

class JoinProcessor {
    private BlockingQueue<SalesRecord> transactionStreamBuffer;
    private HashMap<Integer, SalesRecord> multiHashTable;
    private BlockingQueue<Integer> joinQueue;
    private HashMap<Integer, ProductInfo> diskBuffer;

    public JoinProcessor(BlockingQueue<SalesRecord> transactionStreamBuffer,
                         HashMap<Integer, SalesRecord> multiHashTable,
                         BlockingQueue<Integer> joinQueue,
                         HashMap<Integer, ProductInfo> diskBuffer) {
        this.transactionStreamBuffer = transactionStreamBuffer;
        this.multiHashTable = multiHashTable;
        this.joinQueue = joinQueue;
        this.diskBuffer = diskBuffer;
    }

    public void process() {
        try {
            do {
                SalesRecord sale = transactionStreamBuffer.take();
                int attributeval = sale.getProductID();

                // Load data into multi-hash table and join queue
                multiHashTable.put(attributeval, sale);
                joinQueue.put(attributeval);

                // Select the oldest node from the join queue
                int firstnode = joinQueue.take();

                // Load the corresponding segment of MD into the disk buffer
                loadDiskBuffer(firstnode);

                // Match MD tuples with the multi-hash table and produce the output tuple
                processJoinOutput(firstnode);
            } while (true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void loadDiskBuffer(int attributeval) {
        String databaseName = "electronica";
        String dbusername = "root";
        String dbpassword = "123456";

        try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + databaseName, dbusername, dbpassword)) {
            String query = "SELECT * FROM dim_products WHERE product_id = ?";

            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setInt(1, attributeval);

                try (ResultSet res = preparedStatement.executeQuery()) {
                    do {
                        if (res.next()) {
                            int productId = res.getInt("product_id");
                            String productName = res.getString("product_name");
                            double productPrice = res.getDouble("product_price");
                            int supplierId = res.getInt("supplier_id");
                            String supplierName = res.getString("supplier_name");
                            int storeId = res.getInt("store_id");
                            String storeName = res.getString("store_name");

                            ProductInfo productInfo = new ProductInfo(productId, productName, productPrice, supplierId, supplierName, storeId, storeName);
                            diskBuffer.put(attributeval, productInfo);
                        }
                    } while (res.next());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void processJoinOutput(int attributeval) {
        SalesRecord sale = multiHashTable.get(attributeval);
        ProductInfo productInfo = diskBuffer.get(attributeval);

        if (sale != null && productInfo != null) {
            int orderId = sale.getOrderId();
            int customerID = sale.getCustomerID();
            int productID = sale.getProductID();
            String customerName = sale.getCustomerName();
            String gender = sale.getGender();
            int quantityOrdered = sale.getQuantityOrdered();

            String productName = productInfo.getProductName();
            double productPrice = productInfo.getProductPrice();
            String supplierName = productInfo.getSupplierName();
            String storeName = productInfo.getStoreName();

            double totalSale = calculateSalesSum(quantityOrdered, productPrice);

            printJoinResult(orderId, customerID, productID, customerName, gender, quantityOrdered, productName, productPrice, supplierName, storeName, totalSale);
        }
    }

    private double calculateSalesSum(int quantityOrdered, double productPrice) {
        return quantityOrdered * productPrice;
    }

    private void printJoinResult(int orderId, int customerID, int productID, String customerName, String gender, int quantityOrdered, String productName, double productPrice, String supplierName, String storeName, double totalSale) {
        System.out.println("Join Result:");
        System.out.println("Order ID: " + orderId);
        System.out.println("Customer ID: " + customerID);
        System.out.println("Product ID: " + productID);
        System.out.println("Customer Name: " + customerName);
        System.out.println("Gender: " + gender);
        System.out.println("Quantity Ordered: " + quantityOrdered);
        System.out.println("Product Name: " + productName);
        System.out.println("Product Price: " + productPrice);
        System.out.println("Supplier Name: " + supplierName);
        System.out.println("Store Name: " + storeName);
        System.out.println("Total Sale: " + totalSale);
    }

    public void startProcessing() {
        Thread joinProcessorThread = new Thread(this::process);
        joinProcessorThread.start();
    }

    public void start() {
        Thread joinProcessorThread = new Thread(this::process);
        joinProcessorThread.start();
    }
}

class DataGenerator extends Thread {
    private BlockingQueue<SalesRecord> transactionStreamBuffer;

    public DataGenerator(BlockingQueue<SalesRecord> transactionStreamBuffer) {
        this.transactionStreamBuffer = transactionStreamBuffer;
    }

    public void run() {
        try {
            do {
                int i = 0;
                SalesRecord sale = new SalesRecord(i, i, i, i, i);
                transactionStreamBuffer.put(sale);
                Thread.sleep(1000);
            } while (true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void setProcessingSpeed(int streamArrivalRate, int serviceRate) {
        /*
            int sleepTime = calculateSleepTime(streamArrivalRate, serviceRate);
            controller.setArrivalRate(streamArrivalRate);
            controller.setServiceRate(serviceRate);
        }*/
    }
}

class MainController extends Thread {
    private DataGenerator dataGenerator;
    private JoinProcessor joinProcessor;
    private int transactionCounter;
    private int processedTransactionCounter;

    public MainController(DataGenerator dataGenerator, JoinProcessor joinProcessor) {
        this.dataGenerator = dataGenerator;
        this.joinProcessor = joinProcessor;
    }

    public void run() {
        try {
            do {
                int streamArrivalRate = PredictArrivalRate();
                int serviceRate = PredictServiceRate();
                this.transactionCounter = 0;
                dataGenerator.setProcessingSpeed(streamArrivalRate, serviceRate);
                Thread.sleep(5000);
            } while (true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int PredictArrivalRate() {
        int currentCounterValue = transactionCounter;
        transactionCounter = 0;
        return currentCounterValue;
    }

    public int PredictServiceRate() {
        int currentCounterValue = processedTransactionCounter;
        processedTransactionCounter = 0;
        return currentCounterValue;
    }
}