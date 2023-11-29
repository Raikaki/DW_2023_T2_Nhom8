package dao;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

public class ControlDao {


    public static String getIpDb(int controlId, Connection connection, String name) {
        String ipDb = null;
        String procedureCall = "{CALL GetIpDb(?, ?, ?)}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            callableStatement.setInt(1, controlId);
            callableStatement.setString(2, name);
            callableStatement.registerOutParameter(3, Types.VARCHAR);

            // Execute the stored procedure
            callableStatement.execute();

            // Retrieve the output parameter value
            ipDb = callableStatement.getString(3);

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return ipDb;
    }

    public static String getUserName(Connection connection, int idConfig, String name) {
        String userName = null;
        String procedureCall = "{CALL GetUserName(?, ?,?)}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            callableStatement.setInt(1, idConfig);
            callableStatement.setString(2, name);
            callableStatement.registerOutParameter(3, Types.VARCHAR);


            callableStatement.execute();
            userName = callableStatement.getString(3);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return userName;
    }

    public static String getPassword(Connection connection, int idConfig,String name) {
        String password = null;
        String procedureCall = "{CALL GetPassword(?, ?,?)}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            callableStatement.setInt(1, idConfig);
            callableStatement.setString(2, name);
            callableStatement.registerOutParameter(3, Types.VARCHAR);

            callableStatement.execute();
            password = callableStatement.getString(3);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return password;
    }


    public static String getSourcePath(Connection connection, int id) {
        String sourcePath = null;
        String procedureCall = "{CALL GetSourcePath(?, ?)}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            callableStatement.setInt(1, id);
            callableStatement.registerOutParameter(2, Types.VARCHAR);

            callableStatement.execute();
            sourcePath = callableStatement.getString(2);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return sourcePath;
    }
    public static int getConfigId(Connection connection, String configName) throws SQLException {
        int id = -1;
        String callProcedure = "{CALL GetConfigId(?, ?)}";

        try (CallableStatement callableStatement = connection.prepareCall(callProcedure)) {
            callableStatement.setString(1, configName);
            callableStatement.registerOutParameter(2, Types.INTEGER);

            callableStatement.execute();
            id = callableStatement.getInt(2);
        }

        return id;
    }

    public static String getLocationDateDim(Connection connection, int id) throws SQLException {
        String locationDateDim = null;
        String callProcedure = "{CALL GetLocationDateDim(?, ?)}";

        try (CallableStatement callableStatement = connection.prepareCall(callProcedure)) {
            callableStatement.setInt(1, id);
            callableStatement.registerOutParameter(2, Types.VARCHAR);

            callableStatement.execute();
            locationDateDim = callableStatement.getString(2);
        }

        return locationDateDim;
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

    public static void sendErrorEmail(String toAddress, String subject, String body) {
        // Thiết lập thông tin đăng nhập cho email của bạn
        final String username = "tag03173@gmail.com";
        final String password = "hcdat1232580";

        // Thiết lập cài đặt cho sesion
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        // Tạo đối tượng Session với thông tin đăng nhập
        Session session = Session.getInstance(props, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("20130305@st.hcmuaf.edu.vn", "Linh@27092002");
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

}
