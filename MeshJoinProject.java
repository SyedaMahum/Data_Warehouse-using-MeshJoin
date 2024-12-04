package i211662_dwh_project;

import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MeshJoinProject {
    public static void initMessage() {
        System.out.println("Connected to the database successfully!");
    }

    public static int calculateQuarter(int month) {
        if (month < 1 || month > 12) {
            throw new IllegalArgumentException("Invalid month: " + month);
        }
        return (month - 1) / 3 + 1;
    }

    private static double calculateTotalSale(double productPrice, int quantity) {
        return productPrice * quantity;
    }

    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Enter Username:");
            String user = scanner.nextLine();

            System.out.println("Enter Password:");
            String pwd = scanner.nextLine();

            try (Connection con1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/transactions", user, pwd);
                 Connection con2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/products_data", user, pwd);
                 Connection con3 = DriverManager.getConnection("jdbc:mysql://localhost:3306/METRO_DW", user, pwd)) {

                initMessage();
                con3.setAutoCommit(false); // Enable transaction management

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                int transactionCount = 0;
                int masterCount = 0;
                final int TRANSACTION_BATCH = 50;
                final int MASTER_BATCH = 1000;

                String transactionQuery = "SELECT * FROM enriched_transactions LIMIT ?, ?";
                String masterQuery = "SELECT * FROM products_data LIMIT ?, ?";

                try (PreparedStatement transStmt = con1.prepareStatement(transactionQuery);
                     PreparedStatement masterStmt = con2.prepareStatement(masterQuery)) {

                    for (int iter = 0; iter <= 9999; iter++) {
                        if (masterCount >= 100) masterCount = 0;

                        transStmt.setInt(1, transactionCount);
                        transStmt.setInt(2, TRANSACTION_BATCH);
                        masterStmt.setInt(1, masterCount);
                        masterStmt.setInt(2, MASTER_BATCH);

                        try (ResultSet transactionData = transStmt.executeQuery();
                             ResultSet masterData = masterStmt.executeQuery()) {

                            Map<String, List<Map<String, Object>>> transactionsByProduct = new HashMap<>();

                            // Process transactions
                            while (transactionData.next()) {
                                Map<String, Object> transaction = new HashMap<>();
                                String orderDateStr = transactionData.getString("Order Date");
                                LocalDateTime orderDateTime = LocalDateTime.parse(orderDateStr, formatter);
                                
                                transaction.put("Order_ID", transactionData.getString("Order ID"));
                                transaction.put("ProductID", transactionData.getString("ProductID"));
                                transaction.put("Quantity", Integer.parseInt(transactionData.getString("Quantity Ordered")));
                                transaction.put("customer_id", transactionData.getString("customer_id"));
                                transaction.put("customer_name", transactionData.getString("customer_name"));
                                transaction.put("Order_Date", orderDateTime);
                             

                                String productId = (String) transaction.get("ProductID");
                                transactionsByProduct
                                    .computeIfAbsent(productId, k -> new ArrayList<>())
                                    .add(transaction);
                            }

                            // Process master data
                            while (masterData.next()) {
                                String productId = masterData.getString("productID");
                                List<Map<String, Object>> matchingTransactions = 
                                    transactionsByProduct.getOrDefault(productId, Collections.emptyList());

                                if (!matchingTransactions.isEmpty()) {
                                	 System.out.println("\nMaster Data: ");
                                     System.out.println("Product ID: " + productId);
                                     System.out.println("Product Name: " + masterData.getString("productName"));
                                     System.out.println("Product Price: " + masterData.getDouble("productPrice"));
                                     System.out.println("Supplier ID: " + masterData.getString("supplierID"));
                                     System.out.println("Store ID: " + masterData.getString("storeID"));

                                    try {
                                        // Insert into dimension tables
                                        insertProduct(con3, productId, 
                                                    masterData.getString("productName"),
                                                    masterData.getDouble("productPrice"));
                                        
                                        insertSupplier(con3, 
                                                      masterData.getString("supplierID"),
                                                      masterData.getString("supplierName"));
                                        
                                        insertStore(con3, 
                                                   masterData.getString("storeID"),
                                                   masterData.getString("storeName"));

                                        for (Map<String, Object> transaction : matchingTransactions) {
                                            LocalDateTime orderDateTime = (LocalDateTime) transaction.get("Order_Date");
                                            String orderId = (String) transaction.get("Order_ID");
                                            
                                            // Insert order data
                                            insertOrderData(con3, orderId, orderDateTime.toLocalDate());
                                            
                                            // Insert customer
                                            insertCustomer(con3, 
                                                         (String) transaction.get("customer_id"),
                                                         (String) transaction.get("customer_name"));

                                            // Calculate sales
                                            int quantity = (Integer) transaction.get("Quantity");
                                            double price = masterData.getDouble("productPrice");
                                            double sales = calculateTotalSale(price, quantity);
                                            
                                            // Insert fact
                                            insertFact(con3, sales, productId, 
                                                      masterData.getString("supplierID"),
                                                      orderDateTime.toLocalDate(),
                                                      masterData.getString("storeID"),
                                                      (String) transaction.get("customer_id"),
                                                      orderId, quantity, sales);
                                        }
                                        con3.commit();
                                    } catch (SQLException e) {
                                        con3.rollback();
                                        throw e;
                                    }
                                }
                            }
                        }
                        transactionCount += TRANSACTION_BATCH;
                        masterCount += MASTER_BATCH;
                    }
                    System.out.println("Data insertion completed successfully.");
                }
                
            } catch (SQLException e) {
                System.err.println("SQL Error: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Database operations
    private static void insertProduct(Connection conn, String productId, String productName, double price) 
            throws SQLException {
        String sql = "INSERT INTO PRODUCT (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE) VALUES (?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE PRODUCT_NAME = VALUES(PRODUCT_NAME), PRODUCT_PRICE = VALUES(PRODUCT_PRICE)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, productId);
            ps.setString(2, productName);
            ps.setDouble(3, price);
            ps.executeUpdate();
        }
    }

    private static void insertCustomer(Connection conn, String customerId, String customerName) 
            throws SQLException {
        String sql = "INSERT INTO CUSTOMER (CUSTOMER_ID, CUSTOMER_NAME) VALUES (?, ?) " +
                    "ON DUPLICATE KEY UPDATE CUSTOMER_NAME = VALUES(CUSTOMER_NAME)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, customerId);
            ps.setString(2, customerName);
            ps.executeUpdate();
        }
    }

    private static void insertSupplier(Connection conn, String supplierId, String supplierName) 
            throws SQLException {
        String sql = "INSERT INTO SUPPLIER (SUPPLIER_ID, SUPPLIER_NAME) VALUES (?, ?) " +
                    "ON DUPLICATE KEY UPDATE SUPPLIER_NAME = VALUES(SUPPLIER_NAME)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, supplierId);
            ps.setString(2, supplierName);
            ps.executeUpdate();
        }
    }

    private static void insertStore(Connection conn, String storeId, String storeName) 
            throws SQLException {
        String sql = "INSERT INTO STORE (STORE_ID, STORE_NAME) VALUES (?, ?) " +
                    "ON DUPLICATE KEY UPDATE STORE_NAME = VALUES(STORE_NAME)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, storeId);
            ps.setString(2, storeName);
            ps.executeUpdate();
        }
    }

    private static void insertOrderData(Connection conn, String orderId, LocalDate orderDate) 
            throws SQLException {
        String sql = "INSERT INTO ORDER_DATA (ORDER_ID, ORDER_DATE) VALUES (?, ?) " +
                    "ON DUPLICATE KEY UPDATE ORDER_DATE = VALUES(ORDER_DATE)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, orderId);
            ps.setDate(2, Date.valueOf(orderDate));
            ps.executeUpdate();
        }
    }

    private static void insertFact(Connection conn, double sales, String productId, String supplierId,
                                 LocalDate date, String storeId, String customerId, String orderId,
                                 int quantity, double totalSale) throws SQLException {
        String sql = "INSERT INTO FACT (sales, FK_PRODUCT_ID, FK_SUPPLIER_ID, FK_date, FK_STORE_ID, " +
                    "FK_CUSTOMER_ID, FK_ORDER_ID, Quantity, total_sale) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setDouble(1, sales);
            ps.setString(2, productId);
            ps.setString(3, supplierId);
            ps.setDate(4, Date.valueOf(date));
            ps.setString(5, storeId);
            ps.setString(6, customerId);
            ps.setString(7, orderId);
            ps.setInt(8, quantity);
            ps.setDouble(9, totalSale);
            ps.executeUpdate();
        }
    }
}