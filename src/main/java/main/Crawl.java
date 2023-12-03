package main;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import dao.ControlDao;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class Crawl {
    private static String CONFIG_DB_URL = "";
    private static String CONFIG_USERNAME = "";
    private static String CONFIG_PASSWORD = "";
    private static String tableNameConfig = "";
    private static String email_send_error = "";
    private static String pass_email_error = "";
    private static String email_error = "";
    static {
        //1. Đọc dl từ file properties
        Properties properties = new Properties();
        try (InputStream input = ETLProcess.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new IOException("Sorry, unable to find config.properties");
            }
            // Load a properties file from class path
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        CONFIG_DB_URL = properties.getProperty("db_name");
        CONFIG_USERNAME = properties.getProperty("username");
        CONFIG_PASSWORD = properties.getProperty("password");
        tableNameConfig = properties.getProperty("nameConfig");
        email_send_error = properties.getProperty("email_send_error");
        pass_email_error = properties.getProperty("pass_email_error");
        email_error = properties.getProperty("email_error");


    }
        //2. kết nối config database
    static Connection configConnection;

    static {
        try {
            configConnection = DriverManager.getConnection(CONFIG_DB_URL, CONFIG_USERNAME, CONFIG_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    private static final int id_config;

    static {
        try {
            id_config = ControlDao.getConfigId(configConnection, tableNameConfig);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    static String email_error_config = getErrorEmail(id_config,tableNameConfig,configConnection);
    public static void main(String[] args) {
        try {
            // Check the current status before proceeding
            //3. kiểm tra status = finish nơi có name SO XO (tableNameConfig)
            String currentStatus = getCurrentStatus(configConnection, tableNameConfig);

            if ("FINISH".equals(currentStatus)) {
                updateStatus(configConnection, "CRAWLING");
                moveOldFilesToYesterdayFolder();
                crawlSource();
                updateStatus(configConnection, "SAVED");
            } else {
                System.out.println("The process will not run as the current status is not FINISH.");
                updateStatus(configConnection, "ERROR");
                insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(),
                        "An error occurred while crawling from web.","");
                sendErrorEmail(email_error_config, "PROCESS ERROR" + "from" + tableNameConfig,
                        "Process error.." + "from" + Thread.currentThread().getName() + "\n" );
            }

        } catch (IOException e) {
            // Handle IO exception
            e.printStackTrace();
            updateStatus(configConnection, "ERROR");
            insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(),
                    "An error occurred while crawling from web.", e.getMessage());
            sendErrorEmail(email_error_config, "PROCESS ERROR" + "from" + tableNameConfig,
                    "Process error.." + "from" + Thread.currentThread().getName() + "\n" + e.getMessage());

        }
    }

    // Modify this method to retrieve the current status from the database using a SELECT statement
    // Kiềm tra flag = 1 thì run, name: SO XO
    private static String getCurrentStatus(Connection connection, String tableName) {
        String status = null;

        try (PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT status FROM config.config WHERE flags = 1 and name = ?")) {
            preparedStatement.setString(1, tableName);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    status = resultSet.getString("status");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // Handle the exception appropriately
        }

        return status;
    }

    public static String getErrorEmail(int idConfig, String name, Connection connection) {
        String errorEmail = null;
        String procedureCall = "{CALL GetErrorEmail(?, ?, ?)}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            callableStatement.setInt(1, idConfig);
            callableStatement.setString(2, name);
            callableStatement.registerOutParameter(3, Types.VARCHAR);

            callableStatement.execute();

            // Retrieve the value of the output parameter
            errorEmail = callableStatement.getString(3);

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return errorEmail;
    }

    // hàm gọi api gửi mail
    public static void sendErrorEmail(String toAddress, String subject, String body) {
        // Thiết lập thông tin đăng nhập cho email của bạn
        final String username = email_send_error;
        final String password = pass_email_error;

        // Thiết lập cài đặt cho sesion
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        // Tạo đối tượng Session với thông tin đăng nhập
        Session session = Session.getInstance(props, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("20130332@st.hcmuaf.edu.vn", "giangan411@");
            }
        });
        try {
            // Tạo đối tượng MimeMessage
            Message message = new MimeMessage(session);

            // Đặt địa chỉ người gửi
            message.setFrom(new InternetAddress(username));

            // Đặt địa chỉ người nhận
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toAddress));

            // Đặt chủ đề email
            message.setSubject(subject);

            // Đặt nội dung email
            message.setText(body);

            // Gửi email
            Transport.send(message);

            System.out.println("Email sent successfully to " + toAddress);

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }
    private static void insertLog(String logLevel, String loggerName, String threadName, String message, String exception) {
        try (CallableStatement insertLogProcedure = configConnection.prepareCall("{CALL InsertLog(?, ?, ?, ?, ?)}")) {
            insertLogProcedure.setString(1, logLevel);
            insertLogProcedure.setString(2, loggerName);
            insertLogProcedure.setString(3, threadName);
            insertLogProcedure.setString(4, message);
            insertLogProcedure.setString(5, exception);

            insertLogProcedure.execute();
        } catch (SQLException e) {
            // Handle the exception appropriately
            e.printStackTrace();
        }
    }
    private static void updateStatus(Connection connection,String newStatus) {

        try (CallableStatement updateStatusProcedure = connection.prepareCall("{CALL UpdateStatus(?, ?)}")) {
            updateStatusProcedure.setString(1, tableNameConfig);
            updateStatusProcedure.setString(2, newStatus);

            updateStatusProcedure.execute();

        } catch (SQLException e) {

        }
    }

    private static void crawlSource() throws IOException {
        // Kết nối và lấy dữ liệu từ trang web
        String link = "https://xskt.com.vn/xsmn";
        Document doc = Jsoup.connect(link).timeout(500000).get();
        Elements comicElements = doc.select(".box-ketqua");

        // Lấy ngày hiện tại để tạo tên file CSV
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String currentDate = dateFormat.format(new Date());

        // Tạo tên file CSV dựa trên ngày hiện tại
        String csvFileName = "data" + currentDate + ".csv";

        // Kiểm tra xem file cùng tên đã tồn tại chưa
        File existingFile = new File(csvFileName);
        if (existingFile.exists()) {
            existingFile.delete(); // Xóa file cũ nếu đã tồn tại
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFileName))) {
            // Ghi header (tên cột) vào file CSV
            writer.write("Thứ,Ngày,Tỉnh,Giải,Số,Loại xổ số");
            writer.newLine();

            // Lặp qua các phần tử trên trang web để lấy thông tin
            for (Element comicElement : comicElements) {
                // Lấy Thứ
                Element thuElement = comicElement.select("h2 a:last-child").first();
                String thu = (thuElement != null) ? thuElement.text() : "N/A";

                // Lấy Ngày
                Element ngayElement = comicElement.select("h2 a:nth-child(2)").first();
                String ngay = (ngayElement != null) ? ngayElement.text() : "N/A";

                // Lấy thông tin từ bảng kết quả
                Element tableElement = comicElement.select(".box-table table tbody").first();

                if (tableElement != null) {
                    // Lấy loại xổ số
                    Elements loaiXoSoElements = comicElement.select("h2 a");
                    String loaiXoSo = (loaiXoSoElements != null && loaiXoSoElements.size() > 0) ? loaiXoSoElements.get(0).text() : "N/A";

                    // Lấy tất cả các tỉnh
                    Elements tinhElements = tableElement.select("th a");
                    for (int i = 0; i < tinhElements.size(); i++) {
                        String tinh = tinhElements.get(i).text();

                        // Lấy tất cả các giải và số
                        Elements rowElements = tableElement.select("tr");
                        for (Element row : rowElements) {
                            try {
                                // Lấy Giải, Số, Loại xổ số
                                String giai = row.select("td:nth-child(1)").text();

                                // Check if the current row belongs to the desired province
                                if (i < row.select("td").size()) {
                                    Element numbersElement = row.select("td:nth-child(" + (i + 1) + ")").first();
                                    String[] numbers = numbersElement.html().split("<br>");

                                    writeToCSV(writer, thu, ngay, tinh, loaiXoSo, giai, numbers);
                                }
                            } catch (Exception e) {
                                // Xử lý ngoại lệ khi parsing dòng
                            }
                        }
                    }
                    // Break sau khi xử lý bảng đầu tiên
                    break;
                }

            }
            updateLocationInConfig(existingFile.getAbsolutePath(), configConnection, tableNameConfig);
            System.out.println("Dữ liệu đã được crawl và lưu vào file CSV thành công!");
        }
    }

    private static void writeToCSV(BufferedWriter writer, String thu, String ngay, String tinh, String loaiXoSo, String giai, String[] numbers) throws IOException {
        for (String so : numbers) {
            try {
                // Lấy nội dung văn bản của số mà không bao gồm thẻ HTML
                String soWithoutHtml = Jsoup.parse(so).text();

                // Add condition to check if the values are not empty or undesired
                if (!soWithoutHtml.trim().isEmpty() && !soWithoutHtml.trim().equals("G.8") && !soWithoutHtml.trim().equals("G.7") && !soWithoutHtml.trim().equals("G.6") && !soWithoutHtml.trim().equals("G.5") && !soWithoutHtml.trim().equals("G.4") && !soWithoutHtml.trim().equals("G.3") && !soWithoutHtml.trim().equals("G.2") && !soWithoutHtml.trim().equals("G.1") && !soWithoutHtml.trim().equals("ĐB")) {
                	 // In thông tin chi tiết
//                  System.out.println("Thứ: " + thu);
//                  System.out.println("Ngày: " + ngay);
//                  System.out.println("Tỉnh: " + tinh);
//                  System.out.println("Giải: " + giai);
//                  System.out.println("Số: " + soWithoutHtml.trim());
//                  System.out.println("Loại xổ số: " + loaiXoSo);
//                  System.out.println("------------------------");

                	
                	// Ghi dữ liệu vào file CSV
                    String csvLine = thu + "," + ngay + "," + tinh + "," + giai + "," + soWithoutHtml.trim() + "," + loaiXoSo;
                    writer.write(csvLine);
                    writer.newLine();
                }

            } catch (Exception e) {
                // Xử lý ngoại lệ khi parsing số
            }
        }
    }


    private static void moveOldFilesToYesterdayFolder() {
    	// Kiểm tra nếu thư mục "yesterday" chưa tồn tại, thì tạo mới
        File yesterdayFolder = new File("yesterday");
        yesterdayFolder.mkdir();
        
        // Di chuyển tất cả các file cũ vào thư mục "yesterday"
        File[] oldFiles = new File(".").listFiles((dir, name) -> name.startsWith("data") && name.endsWith(".csv"));
        for (File oldFile : oldFiles) {
            Path source = oldFile.toPath();
            Path destination = new File(yesterdayFolder, oldFile.getName()).toPath();
            try {
                Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
                System.out.println("Đã chuyển tập tin ngày hôm trước sang: " + destination.toAbsolutePath());
            } catch (IOException e) {
                System.err.println("Quá trình chuyển tập tin ngày hôm trước bị lỗi: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    private static void updateLocationInConfig(String location, Connection connection, String tableName) {
        try (PreparedStatement updateLocationStatement = connection.prepareStatement(
                "UPDATE config SET location = ? WHERE name = ?")) {
            updateLocationStatement.setString(1, location);
            updateLocationStatement.setString(2, tableName);
            updateLocationStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            // Handle the exception appropriately
        }
    }
}
