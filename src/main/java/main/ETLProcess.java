package main;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import dao.ControlDao;

public class ETLProcess {

    private static final Logger logger = LogManager.getLogger(ETLProcess.class);

    private static String CONFIG_DB_URL = "";

    private static String CONFIG_USERNAME = "";
    private static String CONFIG_PASS = "";
    private static String Name_staging = "";
    private static String Name_warehouse= "";
    private static String tableNameConfig;
    private static String email_error;

    static {
        Properties properties = new Properties();
        try (InputStream input = ETLProcess.class.getClassLoader().getResourceAsStream("config.properties")) {
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

    static Connection configConnection;

    static {
        try {
            configConnection = DriverManager.getConnection(CONFIG_DB_URL, CONFIG_USERNAME, CONFIG_PASS);
        } catch (SQLException e) {
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



    static String url;

    static {
        url = ControlDao.getSourcePath(configConnection, id_config);
    }

    private static String USERNAMESTAGING;
    private static String USERNAMEWAREHOUSE;

    static {
        USERNAMESTAGING = ControlDao.getUserName(configConnection, id_config, Name_staging);
    }

    static {
        USERNAMEWAREHOUSE = ControlDao.getUserName(configConnection, id_config, Name_warehouse);
    }

    private static String PASSWORDSTAGING;

    static {
        PASSWORDSTAGING = ControlDao.getPassword(configConnection, id_config, Name_staging);
    }

    private static String PASSWORDWAREHOUSE;

    static {
        PASSWORDWAREHOUSE = ControlDao.getPassword(configConnection, id_config, Name_warehouse);
    }

    private static String STAGING_DB_URL = "jdbc:mysql://" + ControlDao.getIpDb(id_config, configConnection, Name_staging);
    private static String DW_DB_URL = "jdbc:mysql://" + ControlDao.getIpDb(id_config, configConnection, Name_warehouse);
    static String email_error_config = ControlDao.getErrorEmail(id_config,tableNameConfig,configConnection);

    public static void main(String[] args) {
        boolean etlSuccess = true; // Biến kiểm tra trạng thái
        System.out.println(STAGING_DB_URL);
        System.out.println(DW_DB_URL);

        try (Connection configConnection = DriverManager.getConnection(CONFIG_DB_URL, CONFIG_USERNAME, CONFIG_PASS)) {
            configConnection.setAutoCommit(false); // Bắt đầu transaction
            logger.info("ETL process started.");
            insertLog("INFO", "PROCESS START", Thread.currentThread().getName(), "Process start..", null);
            if (isStatusSuccess(configConnection, tableNameConfig)) {
                try (Connection stagingConnection = DriverManager.getConnection(STAGING_DB_URL, USERNAMESTAGING, PASSWORDSTAGING);
                     Connection dwConnection = DriverManager.getConnection(DW_DB_URL, USERNAMEWAREHOUSE, PASSWORDWAREHOUSE)) {

                    etlSuccess = processDimDateFromCSV(dwConnection, ControlDao.getLocationDateDim(configConnection, id_config))
                            && processDimDay(stagingConnection, dwConnection)
                            && processDimProvince(stagingConnection, dwConnection)
                            && processDimPrize(stagingConnection, dwConnection)
                            && processDimType(stagingConnection, dwConnection);

                    if (etlSuccess && transformAndLoadFactData(dwConnection)) {
                        configConnection.commit(); // Commit transaction nếu mọi thứ thành công
                        updateStatus(configConnection, "fact", "INSERTED");
                        insertLog("ALERT", "PROCESS FINISH", Thread.currentThread().getName(), "Process finish..", null);
                        cleanStagingTable();
                    } else {
                        configConnection.rollback(); // Rollback nếu có lỗi

                        insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Process error..", null);
                    }

                } catch (SQLException e) {
                    configConnection.rollback(); // Rollback nếu có lỗi trong try-with-resources
                    logger.error("An error occurred during the ETL process.", e);
                    ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR"+"from"+tableNameConfig,"Process error.."+"from"+Thread.currentThread().getName()+"\n"+e.getMessage());
                    insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Process error..", e.getMessage());
                    updateStatus(configConnection, "fact", "ERROR");
                    etlSuccess = false;
                }

                if (!etlSuccess) {
                    logger.error("ETL process encountered an error. Aborting.");
                    updateStatus(configConnection, "fact", "ERROR");
                    ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR"+"from"+tableNameConfig,"Process error.."+"from"+Thread.currentThread().getName()+"\n");

                    insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "ETL process encountered an error. Aborting.", null);
                } else {
                    logger.info("ETL process completed successfully.");
                }
            } else {
                logger.warn("ETL process cannot start. Status is not 'success'.");
                updateStatus(configConnection, "fact", "ERROR");
                ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR "+" from "+tableNameConfig,"Process error.."+"from "+Thread.currentThread().getName()+"\n"+"ETL process cannot start. Status is not 'success'.");

                insertLog("WARNING", "PROCESS ERROR", Thread.currentThread().getName(), "ETL process cannot start. Status is not 'success'.", null);

            }
        } catch (SQLException e) {
            logger.error("An error occurred while connecting to config.db.", e);
            insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "An error occurred while connecting to config.db.", e.getMessage());
            ControlDao.sendErrorEmail(email_error,"PROCESS ERROR ","Process error.."+"\n"+"ETL process cannot start.Cannot access config.db");
        }
    }

    private static boolean processDimDateFromCSV(Connection dwConnection, String csvFilePath) {
        try {
            logger.info("Inserting dim_date table from CSV...");
            updateStatus(configConnection, "dim_date", "READING");

            // Call the stored procedure for each row in the CSV
            try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath))) {
                String[] nextLine;
                while ((nextLine = csvReader.readNext()) != null) {
                    // Assuming the CSV file structure matches the table columns
                    try (CallableStatement insertDimDateProcedure = dwConnection.prepareCall("{CALL InsertDimDateFromCSV(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {
                        insertDimDateProcedure.setInt(1, Integer.parseInt(nextLine[0])); // date_key
                        insertDimDateProcedure.setDate(2, Date.valueOf(nextLine[1])); // date
                        insertDimDateProcedure.setInt(3, Integer.parseInt(nextLine[2])); // day_since_2023
                        insertDimDateProcedure.setInt(4, Integer.parseInt(nextLine[3])); // month_since_2023
                        insertDimDateProcedure.setString(5, nextLine[4]); // day_of_week
                        insertDimDateProcedure.setString(6, nextLine[5]); // calendar_month
                        insertDimDateProcedure.setString(7, nextLine[6]); // calendar_year
                        insertDimDateProcedure.setString(8, nextLine[7]); // calendar_year_month
                        insertDimDateProcedure.setInt(9, Integer.parseInt(nextLine[8])); // day_of_month
                        insertDimDateProcedure.setInt(10, Integer.parseInt(nextLine[9])); // day_of_year
                        insertDimDateProcedure.setInt(11, Integer.parseInt(nextLine[10])); // week_of_year_sunday
                        insertDimDateProcedure.setString(12, nextLine[11]); // year_week_sunday
                        insertDimDateProcedure.setDate(13, Date.valueOf(nextLine[12])); // week_sunday_start
                        insertDimDateProcedure.setInt(14, Integer.parseInt(nextLine[13])); // week_of_year_monday
                        insertDimDateProcedure.setString(15, nextLine[14]); // year_week_monday
                        insertDimDateProcedure.setDate(16, Date.valueOf(nextLine[15])); // week_monday_start
                        insertDimDateProcedure.setString(17, nextLine[16]); // holiday
                        insertDimDateProcedure.setString(18, nextLine[17]); // day_type
                        insertDimDateProcedure.executeUpdate();
                    }
                }

                return true;
            }

        } catch (SQLException | IOException | CsvValidationException | NumberFormatException e) {
            updateStatus(configConnection, "dim_date", "ERROR");
            logger.error("Error inserting dim_date table from CSV.", e);
            ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR "+" from "+tableNameConfig,"Error inserting dim_date table from CSV. "+" from "+Thread.currentThread().getName()+"\n"+e.getMessage());

            insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Error inserting dim_date table from CSV.", e.getMessage());
        }
        return false;
    }


    private static boolean processDimDay(Connection stagingConnection, Connection dwConnection) {

        logger.info("Processing dim_day table...");
        updateStatus(configConnection, "dim_day", "READING ");
        String storedProcedureCall = "{call process_dim_day_proc()}";
        try (CallableStatement callableStatement = dwConnection.prepareCall(storedProcedureCall)) {
            callableStatement.execute();
            System.out.println("Stored procedure executed successfully.");
            return true;

        } catch (SQLException e) {
            logger.error("Error processing dim_day table.", e);
            insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Error processing dim_day table.", e.getMessage());
            ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR "+" from "+tableNameConfig,"Error processing dim_day table. "+" from "+Thread.currentThread().getName()+"\n"+e.getMessage());
        }
        return false;
    }

    private static boolean processDimProvince(Connection stagingConnection, Connection dwConnection) {

        logger.info("Processing dim_province table...");
        updateStatus(configConnection, "DimProvince", "READING");

        try (CallableStatement callableStatement = dwConnection.prepareCall("{CALL ProcessDimProvince()}")) {
            callableStatement.execute();
            logger.info("Stored procedure ProcessDimProvince executed successfully.");
            return true;

        } catch (SQLException e) {
            updateStatus(configConnection, "DimProvince", "ERROR");
            insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Error processing dim_province table.", e.getMessage());
            logger.error("Error processing dim_province table.", e);
            ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR "+" from "+tableNameConfig,"Error processing dim_province table. " +" from "+Thread.currentThread().getName()+"\n"+e.getMessage());
            return false;

        }
    }

    public static void cleanStagingTable() {
        String cleanProcedure = "{CALL CleanStagingTable()}";

        try (Connection connection = DriverManager.getConnection(STAGING_DB_URL, USERNAMESTAGING, PASSWORDSTAGING);
             CallableStatement cleanProcedureStatement = connection.prepareCall(cleanProcedure)) {

            cleanProcedureStatement.execute();
            System.out.println("Staging table cleaned successfully.");
            insertLog("WARNING", "CLEANING STAGING", Thread.currentThread().getName(), "Cleaning staging.", null);

        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Error cleaning staging table: " + e.getMessage());
        }
    }

    private static boolean processDimType(Connection stagingConnection, Connection dwConnection) {
        try {
            logger.info("Processing dim_type table...");
            updateStatus(configConnection, "DimType", "READING");

            // Call the stored procedure to process dim_type
            try (CallableStatement processDimTypeStatement = dwConnection.prepareCall("{CALL ProcessDimType()}")) {
                processDimTypeStatement.execute();
                return true;
            }catch (Exception ex){
                ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR "+" from "+tableNameConfig,"Error processing dim_type table. " +" from "+Thread.currentThread().getName()+"\n"+ex.getMessage());

                insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Error processing dim_type table.", ex.getMessage());
                return false;
            }



        } catch (Exception e) {
            logger.error("Error processing dim_type table.", e);
            ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR "+" from "+tableNameConfig,"Error processing dim_type table. " +" from "+Thread.currentThread().getName()+"\n"+e.getMessage());

            insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Error processing dim_type table.", e.getMessage());
            return false;
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

    private static boolean processDimPrize(Connection stagingConnection, Connection dwConnection) {
        try {
            logger.info("Processing dim_prize table...");
            updateStatus(configConnection, "DimPrize", "READING");

            Document document = Jsoup.connect(url).get();

            // Lấy tất cả các hàng trong bảng có class "dstrunggiai"
            Elements rows = document.select("table.dstrunggiai tr");
            Element table = document.select("table#MN6").first();
            Elements row2s = table.select("tr");
            int j = 1;
            for (int i = 1; i < rows.size(); i++) { // Bắt đầu từ 1 để bỏ qua hàng tiêu đề
                Element row = rows.get(i);
                Elements columns = row.select("td");
                Element row2 = row2s.get(rows.size() - i);
                Elements columns2 = row2.select("td");
                String prizeValue = columns.get(3).text();
                System.out.println(prizeValue);
                String prizeName = columns2.get(0).text();
                System.out.println(prizeName);
                try (CallableStatement loadDimPrizeStatement = dwConnection.prepareCall("{CALL LoadDimPrize(?, ?, ?)}")) {
                    loadDimPrizeStatement.setInt(1, i);
                    loadDimPrizeStatement.setString(2, prizeName);
                    loadDimPrizeStatement.setString(3, prizeValue);
                    if (hasDataChangedPrize(dwConnection, j)) {
                        loadDimPrizeStatement.execute();
                    }
                }
                j++;
            }
            return true;

        } catch (Exception d) {
            d.printStackTrace();
            insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Error processing dim_prize .", d.getMessage());
            ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR "+" from "+tableNameConfig,"Error processing dim_prize . " +" from "+Thread.currentThread().getName()+"\n"+d.getMessage());

            return false;
        }
    }

    private static boolean hasDataChangedPrize(Connection dwConnection, int key) throws SQLException {
        // Call the stored function to check if the data has changed
        try (CallableStatement hasDataChangedStatement = dwConnection.prepareCall("{? = CALL HasDataChangedPrize(?)}")) {
            hasDataChangedStatement.registerOutParameter(1, Types.BOOLEAN);
            hasDataChangedStatement.setInt(2, key);

            hasDataChangedStatement.execute();

            return hasDataChangedStatement.getBoolean(1);
        }
    }


    private static boolean transformAndLoadFactData(Connection dwConnection) {
        try {
            logger.info("Transforming and loading fact table...");
            updateStatus(configConnection, "fact", "TRANSFORMING");
            String procedureCall = "{CALL TransformAndLoadFactData()}";
            try (CallableStatement callableStatement = dwConnection.prepareCall(procedureCall)) {

                callableStatement.execute();
                updateStatus(configConnection, "fact", "TRANSFORMED");
                updateStatus(configConnection, "fact", "INSERTING");
            }
            updateStatus(configConnection, "fact", "INSERTED");
            return true;

        } catch (SQLException e) {
            logger.error("Error transforming and loading fact table.", e);
            insertLog("ERROR", "PROCESS ERROR", Thread.currentThread().getName(), "Error transforming and loading fact table.", e.getMessage());
            ControlDao.sendErrorEmail(email_error_config,"PROCESS ERROR "+" from "+tableNameConfig,"Error transforming and loading fact table. " +" from "+Thread.currentThread().getName()+"\n"+e.getMessage());

            return false;
        }
    }


    private static void updateStatus(Connection connection, String tableName, String newStatus) {

        try (CallableStatement updateStatusProcedure = connection.prepareCall("{CALL UpdateStatus(?, ?)}")) {
            updateStatusProcedure.setString(1, tableNameConfig);
            updateStatusProcedure.setString(2, newStatus);

            updateStatusProcedure.execute();
            logger.info("Status updated to '{}' for table '{}'.", newStatus, tableName);
        } catch (SQLException e) {
            logger.error("Error updating status for table '{}'.", tableName, e);
        }
    }

    private static boolean isStatusSuccess(Connection connection, String stepName) {
        boolean isSuccess = false;
        String procedureCall = "{CALL IsStatusSuccess(?, ?)}";

        try (CallableStatement callableStatement = connection.prepareCall(procedureCall)) {
            callableStatement.registerOutParameter(1, Types.BOOLEAN);
            callableStatement.setString(2, stepName);

            callableStatement.execute();
            isSuccess = callableStatement.getBoolean(1);
        } catch (SQLException e) {
            logger.error("Error checking status for step '{}'.", stepName, e);
            insertLog("WARNING", "PROCESS ERROR", Thread.currentThread().getName(), "Error checking status for step " + stepName, e.getMessage());
        }

        return isSuccess;
    }


}
