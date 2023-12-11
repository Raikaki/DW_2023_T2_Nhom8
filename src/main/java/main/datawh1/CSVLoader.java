package datawh1;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class CSVLoader {
    static String dbQueryFilePath = "src\\db.txt";
    static List<String> dbQueries = readDbQueries("src\\db.txt");
    static String insertQuery = dbQueries.get(0);
    static String clearSql = dbQueries.get(1);
    static  String statusSql = dbQueries.get(2);
    static String updateStatusSql = dbQueries.get(3);
    static String readCsvQuery = dbQueries.get(4);
    private static String CONFIG_DB_URL = "";

    private static String CONFIG_USERNAME = "";
    private static String CONFIG_PASS = "";
    private static String Name_staging = "";
    private static String Name_warehouse= "";
    private static String tableNameConfig;
    private static String email_error;

    static {
        Properties properties = new Properties();
        try (InputStream input = CSVLoader.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
            }

            // Load a properties file from class path
            properties.load(input);
        } catch (Exception e) {
            e.printStackTrace(); // handle the exception according to your needs
        }
        CONFIG_DB_URL = properties.getProperty("db_name");
        CONFIG_USERNAME = properties.getProperty("username");
        CONFIG_PASS = properties.getProperty("password");
        Name_staging = properties.getProperty("staging");
        Name_warehouse = properties.getProperty("warehouse");
        tableNameConfig = properties.getProperty("nameConfig");
        email_error = properties.getProperty("email_error");
    }

    public static void main(String[] args) throws SQLException {
        Connection connectionA = DriverManager.getConnection(CONFIG_DB_URL, CONFIG_USERNAME, CONFIG_PASS);
        String csvFilePath = readCsvFilePathFromDatabase(connectionA);

        try {

            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/datawh", "root", "");
            if (checkStatus(connectionA)) {
            clearOldData(connection);
            List<String[]> data = readCSV(csvFilePath);
            saveToDatabase(data, connection);
                updateStatus(connectionA,"Success");
            } else {
                System.err.println("Trạng thái không hợp lệ: ERR");}
            connection.close();
        } catch (SQLException | ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (CsvException e) {
            throw new RuntimeException(e);
        }
    }
    private static List<String[]> readCSV(String filePath) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            return reader.readAll();
        }
    }
    private static void saveToDatabase(List<String[]> data, Connection connection) throws SQLException, ParseException   {
//        String sql = "INSERT INTO staging(thu, ngay, tinh, so, giai, khuvuc, date) " +
//                "VALUES (?, STR_TO_DATE(?, '%d/%m/%Y'), ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
//                "ON DUPLICATE KEY UPDATE " +
//                "thu = VALUES(thu), ngay = VALUES(ngay), tinh = VALUES(tinh), so = VALUES(so), giai = VALUES(giai), khuvuc = VALUES(khuvuc), date = CURRENT_TIMESTAMP";

        try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
            for (String[] row : data) {
                // Kiểm tra giá trị ngày không rỗng và hợp lệ
                if (row[1] != null && !row[1].isEmpty()) {
                    try {
                        Date parsedDate = new SimpleDateFormat("dd/MM/yyyy").parse(row[1]);
                        statement.setString(1, row[0]);
                        statement.setString(2, row[1]);
                        statement.setString(3, row[2]);
                        statement.setString(4, row[3]);
                        statement.setString(5, row[4]);
                        statement.setString(6, row[5]);

                        // Thực hiện truy vấn
                        statement.executeUpdate();
                    } catch (ParseException e) {

                    }
                } else {
                }
            }
        }
    }
    private static void clearOldData(Connection connection) throws SQLException {
//        String clearSql = "TRUNCATE TABLE staging";
        try (PreparedStatement clearStatement = connection.prepareStatement(clearSql)) {
            clearStatement.executeUpdate();
        }
    }
    private static boolean checkStatus(Connection connectionA ) throws SQLException {

//        String statusSql = "SELECT status FROM status WHERE id = ?";
        try (PreparedStatement statusStatement = connectionA.prepareStatement(statusSql)) {
            statusStatement.setInt(1, 1); // Thay thế 1 bằng ID thích hợp trong bảng trạng thái của bạn
            try (ResultSet resultSet = statusStatement.executeQuery()) {
                if (resultSet.next()) {
                    String status = resultSet.getString("status");
                    return "SAVED".equals(status);
                } else {
                    // Nếu không tìm thấy bản ghi, coi như status không hợp lệ
                    return false;
                }
            }
        }
    }
    private static List<String> readDbQueries(String dbQueryFilePath) {
        List<String> dbQueries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(dbQueryFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                dbQueries.add(line);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        return dbQueries;

    }
    private static void updateStatus(Connection connectionA, String newStatus) throws SQLException {

//        String updateStatusSql = "UPDATE status SET status = ? WHERE id = ?";
        try (PreparedStatement updateStatusStatement = connectionA.prepareStatement(updateStatusSql)) {
            updateStatusStatement.setString(1, newStatus);
            updateStatusStatement.setInt(2, 1);
            updateStatusStatement.executeUpdate();
        }
    }
    private static String readCsvFilePathFromDatabase(Connection connectionA) throws SQLException {

        String csvFilePath = null;
//        String query = "SELECT location FROM config";
        try (Statement statement = connectionA.createStatement();
             ResultSet resultSet = statement.executeQuery(readCsvQuery)) {
            if (resultSet.next()) {
                csvFilePath = resultSet.getString("location");
            }
        }
        return csvFilePath;
    }
//        String query = "SELECT location FROM config";

}
