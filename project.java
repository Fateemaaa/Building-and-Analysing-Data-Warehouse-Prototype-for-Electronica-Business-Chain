package proj;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class project {
    public static void main(String[] args) {
        String databaseName = "electronica";
        String dbUsername = "root";
        String dbPassword = "123456";

        try (Connection datac = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + databaseName, dbUsername, dbPassword);
             Connection snowflake = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + databaseName, dbUsername, dbPassword)) {

            // Import data from transactions.csv
            loaddata(datac, "transactions.csv", "fact_sales");

            // Import data from master_data.csv
            loaddata2(snowflake, "master_data.csv", "dim_products");

            System.out.println("Data was imported here successfully!");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void loaddata(Connection conn, String filename, String tab) { //factsales
        if (!emptytable(conn, tab)) {
            System.out.println(tab + " No data import because table is full.");
            return;
        }

        String sql = "INSERT INTO " + tab + " (order_id, order_date, product_id, customer_id, customer_name, gender, quantity_ordered) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (BufferedReader buff = new BufferedReader(new FileReader(filename));
             PreparedStatement state = conn.prepareStatement(sql)) {

            // Read and discard the header myfile
            buff.readLine();

            // Read and insert data into the table
            String myfile;
            while ((myfile = buff.readLine()) != null) {
                try {
                    String[] data = myfile.split(",");
                    int i = 1;
                    while (i <= data.length) {
                        if (i == 2) { // 'order_date' is the second column
                            Timestamp timestamp = parseDateStringToTimestamp(data[i - 1]);
                            state.setTimestamp(i, timestamp);
                        } else {
                            state.setString(i, data[i - 1]);
                        }
                        i++;
                    }
                    state.executeUpdate();
                } catch (SQLException e) {
                    System.err.println("Error inserting data: " + e.getMessage());
                }
            }

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void loaddata2(Connection conn, String filename, String tab) { //masterdata
        if (!emptytable(conn, tab)) {
            System.out.println(tab + " No data import because table is full.");
            return;
        }

        String sql = "INSERT INTO " + tab + " (product_id, product_name, product_price, supplier_id, supplier_name, store_id, store_name) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (BufferedReader buff = new BufferedReader(new FileReader(filename));
             PreparedStatement state = conn.prepareStatement(sql)) {

            // Read and discard the header line
            buff.readLine();

            // Read and insert data into the table
            String myfile;
            while ((myfile = buff.readLine()) != null) {
                try {
                    String[] data = myfile.split(",");
                    int i = 1;
                    while (i <= data.length) {
                        if (i == 3) { 
                            String prod = data[i - 1].replaceAll("[^\\d.]", ""); // Remove non-numeric characters
                            state.setBigDecimal(i, new BigDecimal(prod));
                        } else if (i == 6) {
                            String store_id = data[i - 1].replaceAll("[^\\d.]", "");
                            state.setString(i, store_id);
                        } else {
                            state.setString(i, data[i - 1]);
                        }
                        i++;
                    }
                    state.executeUpdate();
                } catch (SQLException e) {
                    System.err.println("Error inserting master data: " + e.getMessage());
                }
            }

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static boolean emptytable(Connection conn, String tab) {
        String query = "SELECT COUNT(*) FROM " + tab;
        try (PreparedStatement count_query = conn.prepareStatement(query);
             ResultSet res = count_query.executeQuery()) {
            if (res.next()) {
                int rows = res.getInt(1);
                return rows == 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static Timestamp parseDateStringToTimestamp(String dateString) {
        try {
            SimpleDateFormat dates = new SimpleDateFormat("MM/dd/yy HH:mm");
            java.util.Date parsedDate = dates.parse(dateString);
            return new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }
}