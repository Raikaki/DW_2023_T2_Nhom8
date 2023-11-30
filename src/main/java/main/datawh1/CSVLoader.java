package datawh1;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CSVLoader {
    public static void main(String[] args) {
        String csvFilePath = "D:\\Wondershare\\Staging.csv";
        try {
            // Kết nối đến cơ sở dữ liệu MySQL
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/datawh", "root", "");
            // Kiểm tra giá trị "status" từ bảng MySQL
            // Xoá dữ liệu cũ trước khi thêm mới
            if (checkStatus(connection)) {
            clearOldData(connection);

            // Đọc dữ liệu từ tệp CSV
            List<String[]> data = readCSV(csvFilePath);

            // Lưu dữ liệu vào cơ sở dữ liệu MySQL
            saveToDatabase(data, connection);
                updateStatus(connection, "SUC");
            } else {
                System.err.println("Trạng thái không hợp lệ: ERR");}

            // Đóng kết nối
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
        String sql = "INSERT INTO staging(thu, ngay, tinh, so, giai, khuvuc, date) " +
                "VALUES (?, STR_TO_DATE(?, '%d/%m/%Y'), ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                "ON DUPLICATE KEY UPDATE " +
                "thu = VALUES(thu), ngay = VALUES(ngay), tinh = VALUES(tinh), so = VALUES(so), giai = VALUES(giai), khuvuc = VALUES(khuvuc), date = CURRENT_TIMESTAMP";
//        String sql = "INSERT INTO staging(thu, ngay, tinh, so, giai, khuvuc) " +
//                "VALUES (?, STR_TO_DATE(?, '%d/%m/%Y'), ?, ?, ?, ?) " +
//                "ON DUPLICATE KEY UPDATE " +
//                "thu = VALUES(thu), ngay = VALUES(ngay), tinh = VALUES(tinh), so = VALUES(so), giai = VALUES(giai), khuvuc = VALUES(khuvuc)";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (String[] row : data) {
                // Kiểm tra giá trị ngày không rỗng và hợp lệ
                if (row[1] != null && !row[1].isEmpty()) {
                    try {
                        Date parsedDate = new SimpleDateFormat("dd/MM/yyyy").parse(row[1]);
                        statement.setString(1, row[0]); // Giả sử cột đầu tiên là kiểu in
                        statement.setString(2, row[1]); // Giả sử cột thứ hai là kiểu date
                        statement.setString(3, row[2]); // Giả sử cột thứ ba là kiểu String
                        statement.setString(4, row[3]); // Giả sử cột thứ tư là kiểu String
                        statement.setString(5, row[4]); // Giả sử cột thứ năm là kiểu String
                        statement.setString(6, row[5]); // Giả sử cột thứ sáu là kiểu String

                        // Thực hiện truy vấn
                        statement.executeUpdate();
                    } catch (ParseException e) {
                        System.err.println("Ngày tháng không hợp lệ: " + row[1]);
                        // Xử lý theo ý bạn, có thể bỏ qua hoặc thực hiện xử lý khác
                    }
                } else {
                    System.err.println("Giá trị ngày không hợp lệ: " + row[1]);
                    // Xử lý theo ý bạn, có thể bỏ qua hoặc thực hiện xử lý khác
                }
            }
        }
    }

    private static void clearOldData(Connection connection) throws SQLException {
        String clearSql = "TRUNCATE TABLE staging";
        try (PreparedStatement clearStatement = connection.prepareStatement(clearSql)) {
            clearStatement.executeUpdate();
        }
    }
    private static boolean checkStatus(Connection connection) throws SQLException {
        String statusSql = "SELECT status FROM status WHERE id = ?";
        try (PreparedStatement statusStatement = connection.prepareStatement(statusSql)) {
            statusStatement.setInt(1, 1); // Thay thế 1 bằng ID thích hợp trong bảng trạng thái của bạn
            try (ResultSet resultSet = statusStatement.executeQuery()) {
                if (resultSet.next()) {
                    String status = resultSet.getString("status");
                    return "THANHCONG".equals(status);
                } else {
                    // Nếu không tìm thấy bản ghi, coi như status không hợp lệ
                    return false;
                }
            }
        }
    }
    private static void updateStatus(Connection connection, String newStatus) throws SQLException {
        String updateStatusSql = "UPDATE status SET status = ? WHERE id = ?";
        try (PreparedStatement updateStatusStatement = connection.prepareStatement(updateStatusSql)) {
            updateStatusStatement.setString(1, newStatus);
            updateStatusStatement.setInt(2, 1); // Thay thế 1 bằng ID thích hợp trong bảng trạng thái của bạn
            updateStatusStatement.executeUpdate();
        }
    }

}
