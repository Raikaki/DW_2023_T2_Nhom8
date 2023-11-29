package main;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        // Lấy dữ liệu từ trang web và ghi vào tệp Excel
        fetchAndWriteToExcel();

        // Sau khi có dữ liệu trong tệp Excel, lưu vào cơ sở dữ liệu
        importDataFromExcelToDatabase();
    }

    private static void fetchAndWriteToExcel() throws IOException {
        // URL của trang web bạn muốn lấy dữ liệu
        String link = "https://www.nettruyenus.com/";

        // Kết nối và lấy trang web
        Document doc = Jsoup.connect(link).timeout(500000).get();

        // Tạo một tệp Excel mới
        Workbook workbook = new XSSFWorkbook();

        // Tạo một trang trong tệp Excel
        Sheet sheet = workbook.createSheet("Danh sách truyện");

        // Số dòng hiện tại
        int rowNum = 0;

        // Tạo tiêu đề cho các cột
        Row headerRow = sheet.createRow(rowNum++);
        String[] headers = {"Tên truyện", "URL truyện", "Chapter mới nhất", "Thời gian cập nhật", "Lượt xem", "Bình luận", "Yêu thích", "Tác giả", "Tình trạng", "Thể loại", "Nội dung"};
        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
        }

        // Lấy tất cả các phần tử có class "item" (đại diện cho từng bộ truyện).
        Elements comicElements = doc.select(".item");

        // Lặp qua danh sách các phần tử để lấy thông tin của từng bộ truyện và ghi vào tệp Excel
        for (Element comicElement : comicElements) {
            // Lấy tên bộ truyện
            Element titleElement = comicElement.select("h3 a.jtip").first();
            String title = (titleElement != null) ? titleElement.text() : "N/A";

            // Lấy URL của bộ truyện
            String url = (titleElement != null) ? titleElement.attr("href") : "N/A";
            Element imgElement = comicElement.select("img").first();
            String imgUrl = (imgElement != null) ? imgElement.absUrl("src") : "N/A";

            // Lấy số lượt xem
            Element viewsElement = comicElement.select(".fa-eye").first();
            String views = (viewsElement != null) ? viewsElement.nextSibling().toString().trim() : "N/A";

            // Lấy số bình luận
            Element commentsElement = comicElement.select(".fa-comment").first();
            String comments = (commentsElement != null) ? commentsElement.nextSibling().toString().trim() : "N/A";

            // Lấy số lượt yêu thích
            Element likesElement = comicElement.select(".fa-heart").first();
            String likes = (likesElement != null) ? likesElement.nextSibling().toString().trim() : "N/A";

            // Lấy thông tin chapter mới nhất và thời gian cập nhật
            Element latestChapterElement = comicElement.select(".chapter").first();
            String latestChapter = (latestChapterElement != null) ? latestChapterElement.select("a").text() : "N/A";

            // Lấy thời gian cập nhật và chuyển đổi thành ngày giờ cụ thể
            String updateTimeText = (latestChapterElement != null) ? latestChapterElement.select("i.time").text() : "N/A";
            LocalDateTime updateTime = parseUpdateTime(updateTimeText);

            // Định dạng thời gian thành chuỗi
            String formattedUpdateTime = formatLocalDateTime(updateTime);

            // Kiểm tra nếu URL không là "N/A" thì truy cập và lấy thông tin chap truyện
            if (!url.equals("N/A")) {
                Document comicDetail = Jsoup.connect(url).timeout(500000).get();

                // Lấy nội dung truyện
                Element contentElement = comicDetail.select("div.detail-content p").first();
                String content = (contentElement != null) ? contentElement.text() : "N/A";

                // Lấy tên tác giả
                Element authorElement = comicDetail.select("li.author p.col-xs-8").first();
                String author = (authorElement != null) ? authorElement.text() : "N/A";

                // Lấy tình trạng
                Element statusElement = comicDetail.select("li.status p.col-xs-8").first();
                String status = (statusElement != null) ? statusElement.text() : "N/A";

                // Lấy thể loại
                Elements kindElements = comicDetail.select("li.kind p.col-xs-8 a");
                StringBuilder kinds = new StringBuilder();
                for (Element kindElement : kindElements) {
                    String kind = (kindElement != null) ? kindElement.text() : "N/A";
                    kinds.append(kind).append(", ");
                }
                if (kinds.length() > 0) {
                    kinds.setLength(kinds.length() - 2); // Loại bỏ dấu phẩy cuối cùng
                }

                // Khởi tạo một hàng mới trong tệp Excel
                Row row = sheet.createRow(rowNum++);

                // Đặt các giá trị cho các ô trong hàng
                row.createCell(0).setCellValue(title);
                row.createCell(1).setCellValue(imgUrl);
                row.createCell(2).setCellValue(latestChapter);
                row.createCell(3).setCellValue(formattedUpdateTime);
                row.createCell(4).setCellValue(views);
                row.createCell(5).setCellValue(comments);
                row.createCell(6).setCellValue(likes);
                row.createCell(7).setCellValue(author);
                row.createCell(8).setCellValue(status);
                row.createCell(9).setCellValue(kinds.toString());
                row.createCell(10).setCellValue(content);
            }
        }

        LocalDateTime timeex = LocalDateTime.now();

        // Lưu tệp Excel
        try {
            FileOutputStream outputStream = new FileOutputStream(formatLocalDateTime2(timeex) + "danh_sach_truyen.xlsx");
            workbook.write(outputStream);
            outputStream.close();
            System.out.println("Dữ liệu đã được ghi vào tệp Excel.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void importDataFromExcelToDatabase() throws SQLException {
        String configUrl = "jdbc:mysql://localhost:3306/comic_library"; // URL của cơ sở dữ liệu cấu hình
        String configUser = "root";
        String configPassword = "hcdat1232580";

        try (Connection configConnection = DriverManager.getConnection(configUrl, configUser, configPassword)) {
            String getConfigQuery = "SELECT * FROM database_config WHERE id = ?";
            int configId = 1; // ID của bản ghi cấu hình bạn muốn sử dụng (có thể thay đổi)

            try (PreparedStatement preparedStatement = configConnection.prepareStatement(getConfigQuery)) {
                preparedStatement.setInt(1, configId);
                ResultSet resultSet = preparedStatement.executeQuery();

                if (resultSet.next()) {
                    String dbUrl = resultSet.getString("db_url");
                    String dbUsername = resultSet.getString("db_username");
                    String dbPassword = resultSet.getString("db_password");

                    // Sử dụng thông tin cấu hình để kết nối đến server cơ sở dữ liệu chính
                    try (Connection mainConnection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
                        System.out.println(mainConnection);

                        // Đoạn mã để đọc dữ liệu từ Excel và ghi vào cơ sở dữ liệu chính
                        importDataFromExcelAndInsertIntoDatabase(mainConnection);

                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void importDataFromExcelAndInsertIntoDatabase(Connection connection) {
        LocalDateTime timeex = LocalDateTime.now();
        try (FileInputStream excelFile = new FileInputStream(formatLocalDateTime2(timeex) + "danh_sach_truyen.xlsx");
             Workbook workbook = new XSSFWorkbook(excelFile)) {

            Sheet sheet = workbook.getSheet("Danh sách truyện");

            for (Row row : sheet) {
                // Bỏ qua dòng tiêu đề
                if (row.getRowNum() == 0) {
                    continue;
                }

                // Đọc dữ liệu từ các ô trong hàng
                String title = row.getCell(0).getStringCellValue();
                String imgUrl = row.getCell(1).getStringCellValue();
                String latestChapter = row.getCell(2).getStringCellValue();
                String excelDateTime = row.getCell(3).getStringCellValue();

                // Định dạng chuẩn cho MySQL
                DateTimeFormatter excelFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
                DateTimeFormatter mysqlFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

                // Chuyển đổi từ định dạng Excel sang định dạng MySQL
                LocalDateTime updateTime = LocalDateTime.parse(excelDateTime, excelFormatter);
                String formattedUpdateTime = updateTime.format(mysqlFormatter);

                String views = row.getCell(4).getStringCellValue();
                String comments = row.getCell(5).getStringCellValue();
                String likes = row.getCell(6).getStringCellValue();
                String author = row.getCell(7).getStringCellValue();
                String status = row.getCell(8).getStringCellValue();
                String kinds = row.getCell(9).getStringCellValue();
                String content = row.getCell(10).getStringCellValue();

                // Ghi dữ liệu vào cơ sở dữ liệu MySQL
                String insertQuery = "INSERT INTO comics(title, imgUrl, latestChapter, updateTime, views, comments, likes, author, status, kinds, content) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
                preparedStatement.setString(1, title);
                preparedStatement.setString(2, imgUrl);
                preparedStatement.setString(3, latestChapter);
                preparedStatement.setTimestamp(4, Timestamp.valueOf(formattedUpdateTime));
                preparedStatement.setString(5, views);
                preparedStatement.setString(6, comments);
                preparedStatement.setString(7, likes);
                preparedStatement.setString(8, author);
                preparedStatement.setString(9, status);
                preparedStatement.setString(10, kinds);
                preparedStatement.setString(11, content);

                preparedStatement.executeUpdate();
            }

            System.out.println("Dữ liệu đã được đọc từ Excel và ghi vào cơ sở dữ liệu MySQL.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static LocalDateTime parseUpdateTime(String updateTimeText) {
        LocalDateTime timesago = LocalDateTime.now();
        if (updateTimeText.contains("phút trước")) {
            Pattern pattern = Pattern.compile("(\\d+) phút trước");
            Matcher matcher = pattern.matcher(updateTimeText);
            if (matcher.find()) {
                int minutesAgo = Integer.parseInt(matcher.group(1));
                timesago = LocalDateTime.now().minusMinutes(minutesAgo);
            }
        } else if (updateTimeText.contains("giờ trước")) {
            Pattern pattern = Pattern.compile("(\\d+) giờ trước");
            Matcher matcher = pattern.matcher(updateTimeText);
            if (matcher.find()) {
                int hoursAgo = Integer.parseInt(matcher.group(1));
                timesago = LocalDateTime.now().minusHours(hoursAgo);
            }
        }
        return timesago;
    }

    private static String formatLocalDateTime(LocalDateTime dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
        return dateTime.format(formatter);
    }

    public static String formatLocalDateTime2(LocalDateTime dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        return dateTime.format(formatter);
    }
}
