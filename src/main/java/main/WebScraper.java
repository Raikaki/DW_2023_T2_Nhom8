package main;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

public class WebScraper {
    public static void main(String[] args) {
        String url = "https://xskt.com.vn/xsmn";

        try {
            Document document = Jsoup.connect(url).get();

            // Sử dụng các lệnh select để chọn và lấy dữ liệu từ các phần tử HTML
            Elements rows = document.select("table.dstrunggiai tr");
            System.out.println(rows.size());
            for (int i = 1; i < rows.size(); i++) {
                Element row = rows.get(i);
                Elements columns = row.select("td");

                String slGiai = columns.get(0).text();
                String tenGiai = columns.get(1).text();
                String trung = columns.get(2).text();
                String triGia = columns.get(3).text();

                System.out.println("SL giải: " + slGiai);
                System.out.println("Tên giải: " + tenGiai);
                System.out.println("Trùng: " + trung);
                System.out.println("Trị giá: " + triGia);
                System.out.println("----------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
