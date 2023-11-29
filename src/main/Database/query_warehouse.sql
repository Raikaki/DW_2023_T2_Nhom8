-- Tạo stored procedure
USE warehouse;
DELIMITER
//
CREATE PROCEDURE process_dim_day_proc()
BEGIN
  -- Clear existing data in warehouse.dim_day


  -- Loop through staging data
  DECLARE
done INT DEFAULT FALSE;
  DECLARE
v_day_week VARCHAR(255);
  DECLARE
cur CURSOR FOR
SELECT DISTINCT day_week
FROM staging.staging;
DECLARE
CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

OPEN cur;

SET
@counter = 0;

  read_loop
: LOOP
    FETCH cur INTO v_day_week;

    IF
done THEN
      LEAVE read_loop;
END IF;

    -- Check if data has changed
    IF
hasDataChangedDay(@counter) THEN
      -- Insert data into warehouse.dim_day
      INSERT INTO warehouse.dim_day (day_id, day_value)
      VALUES (@counter, v_day_week);
END IF;

    -- Increment counter
    SET
@counter = @counter + 1;
END LOOP;

CLOSE cur;

-- Call insertLogs function
CALL insertLog('INFO', 'process_dim_day_proc', 'Main Thread', 'Process dim_day completed successfully.', NULL);

END
//

DELIMITER //

DELIMITER //

CREATE PROCEDURE insertLog(
    IN logLevel VARCHAR (10),
    IN loggerName VARCHAR (255),
    IN threadName VARCHAR (255),
    IN logMessage TEXT,
    IN logException TEXT
)
BEGIN
    -- Insert data into logs table
INSERT INTO config.logs (log_level, logger_name, thread_name, message, exception, log_date)
VALUES (logLevel, loggerName, threadName, logMessage, logException, CURRENT_TIMESTAMP);

-- Optionally, you can add additional logic or error handling here

-- If you want to return something from the stored procedure, you can use SELECT
-- For example, you might return the ID of the inserted log record
-- SELECT LAST_INSERT_ID() AS log_id;
END
//

DELIMITER ;

DELIMITER ;

-- Tạo stored function để kiểm tra dữ liệu đã thay đổi
DELIMITER
//
CREATE FUNCTION hasDataChangedDay(dayId INT) RETURNS BOOLEAN
BEGIN
  DECLARE
rowCount INT;
  -- Query to check if the data has changed in the database
SELECT COUNT(*)
INTO rowCount
FROM warehouse.dim_day
WHERE day_id = dayId;

RETURN rowCount = 0; -- If rowCount is greater than 0, data has changed; otherwise, it's the same
END
//
DELIMITER ;
SET
GLOBAL log_bin_trust_function_creators = 1;



DELIMITER
//

CREATE PROCEDURE ProcessDimProvince()
BEGIN
    DECLARE
i INT DEFAULT 1;
    DECLARE
provinceName VARCHAR(255);

    DECLARE
done INT DEFAULT FALSE;

    DECLARE
cur CURSOR FOR
SELECT DISTINCT province
FROM staging.staging;
DECLARE
CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

OPEN cur;

read_loop
: LOOP
        FETCH cur INTO provinceName;

        IF
done THEN
            LEAVE read_loop;
END IF;

        -- Check if data has changed
        IF
HasDataChangedProvince(i) THEN
            -- Insert data into warehouse.dim_province
            INSERT INTO warehouse.dim_province (province_id, province_name)
            VALUES (i, provinceName);
ELSE
            -- Log or perform other actions for existing data
            INSERT INTO log_table (log_level, log_message) VALUES ('INFO', CONCAT('Data already exists for province_id: ', i));
END IF;

        -- Increment counter
        SET
i = i + 1;
END LOOP;

CLOSE cur;
END
//

DELIMITER ;
DELIMITER
//

DELIMITER //
CREATE FUNCTION HasDataChangedProvince(province_key INT) RETURNS BOOLEAN
BEGIN
  DECLARE
rowCount INT;
  -- Query to check if the data has changed in the database
SELECT COUNT(*)
INTO rowCount
FROM warehouse.dim_province
WHERE province_id = province_key;

RETURN rowCount = 0; -- If rowCount is greater than 0, data has changed; otherwise, it's the same
END
//


DELIMITER ;


DELIMITER
//

CREATE PROCEDURE TransformAndLoadFactData()
BEGIN
    DECLARE
v_day_id, v_date_key, v_type_id, v_province_id, v_prize_key INT;
    DECLARE
v_number VARCHAR(255);

    DECLARE
done INT DEFAULT FALSE;
    DECLARE
cur CURSOR FOR
SELECT d1.day_id,
       d2.date_key,
       d5.type_id,
       d3.province_id,
       d4.prize_key,
       s.number
FROM staging.staging s
         LEFT JOIN warehouse.dim_day d1 ON s.day_week = d1.day_value
         LEFT JOIN warehouse.dim_date d2 ON s.date = d2.date
         LEFT JOIN warehouse.dim_prize d4 ON s.price = d4.prize_name
         LEFT JOIN warehouse.dim_type d5 ON s.type = d5.type_name
         LEFT JOIN warehouse.dim_province d3 ON s.province = d3.province_name;

DECLARE
CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

OPEN cur;


myLoop
: LOOP
        FETCH cur INTO v_day_id, v_date_key, v_type_id, v_province_id, v_prize_key, v_number;
        IF
done THEN
            LEAVE myLoop;
END IF;

INSERT INTO fact (day_id, date_key, type_id, province_id, number, prize_key)
VALUES (v_day_id, v_date_key, v_type_id, v_province_id, v_number, v_prize_key);
END LOOP;
CLOSE cur;
END
//

DELIMITER ;

DELIMITER //

CREATE PROCEDURE LoadDimPrize(
    IN prizeKeyParam INT,
    IN prizeNameParam VARCHAR(255),
    IN prizeValueParam VARCHAR(255)
)
BEGIN
    -- Insert data into the dim_prize table
INSERT INTO warehouse.dim_prize (prize_key, prize_name, prize_value)
VALUES (prizeKeyParam, prizeNameParam, prizeValueParam);
END //

DELIMITER ;

DELIMITER //

CREATE FUNCTION HasDataChangedPrize(inPrimaryKey INT) RETURNS BOOLEAN
BEGIN
    DECLARE rowCount INT;

    -- Query to check if the data has changed in the warehouse.dim_prize table
SELECT COUNT(*) INTO rowCount
FROM warehouse.dim_prize
WHERE prize_key = inPrimaryKey;

-- If rowCount is greater than 0, data has changed; otherwise, it's the same
RETURN rowCount = 0;
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE ProcessDimType()
BEGIN
    DECLARE typeValueVar VARCHAR(255);
    DECLARE i INT DEFAULT 1;

    -- Update status to indicate reading

    -- Extract distinct type values from staging
    DECLARE extractDimTypeCursor CURSOR FOR
SELECT DISTINCT type FROM staging.staging;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET i = -1;

OPEN extractDimTypeCursor;

-- Loop through the result set
read_loop: LOOP
        FETCH extractDimTypeCursor INTO typeValueVar;
        IF i = -1 THEN
            CLOSE extractDimTypeCursor;
            LEAVE read_loop;
END IF;

        -- Load data into dim_type
CALL LoadDimType(i, typeValueVar);
SET i = i + 1;
END LOOP;

    -- Update status to indicate completion

END //

DELIMITER //

CREATE PROCEDURE LoadDimType(IN inTypeId INT, IN inTypeName VARCHAR(255))
BEGIN
    DECLARE rowCount INT;

    -- Check if the data has changed
SELECT COUNT(*)
INTO rowCount
FROM warehouse.dim_type
WHERE type_id = inTypeId;

-- Insert data if it has changed
IF rowCount = 0 THEN
        INSERT INTO warehouse.dim_type (type_id, type_name)
        VALUES (inTypeId, inTypeName);
END IF;
END //

DELIMITER //

    DELIMITER //

CREATE PROCEDURE CleanStagingTable()
BEGIN
TRUNCATE TABLE staging.staging; -- Or use DELETE FROM staging.staging; if you prefer
END //

DELIMITER ;
DELIMITER //

CREATE PROCEDURE InsertDimDateFromCSV(IN dateKeyParam INT, IN dateParam DATE, IN daySince2023Param INT,
                                      IN monthSince2023Param INT, IN dayOfWeekParam VARCHAR(255),
                                      IN calendarMonthParam VARCHAR(255), IN calendarYearParam VARCHAR(255),
                                      IN calendarYearMonthParam VARCHAR(255), IN dayOfMonthParam INT,
                                      IN dayOfYearParam INT, IN weekOfYearSundayParam INT,
                                      IN yearWeekSundayParam VARCHAR(255), IN weekSundayStartParam DATE,
                                      IN weekOfYearMondayParam INT, IN yearWeekMondayParam VARCHAR(255),
                                      IN weekMondayStartParam DATE, IN holidayParam VARCHAR(255),
                                      IN dayTypeParam VARCHAR(255))
BEGIN
    DECLARE rowCount INT;

    -- Check if the data has changed
SELECT COUNT(*)
INTO rowCount
FROM warehouse.dim_date
WHERE date_key = dateKeyParam;

-- Insert data if it has changed
IF rowCount = 0 THEN
        INSERT INTO warehouse.dim_date (date_key, date, day_since_2023, month_since_2023,
                                       day_of_week, calendar_month, calendar_year,
                                       calendar_year_month, day_of_month, day_of_year,
                                       week_of_year_sunday, year_week_sunday,
                                       week_sunday_start, week_of_year_monday,
                                       year_week_monday, week_monday_start,
                                       holiday, day_type)
        VALUES (dateKeyParam, dateParam, daySince2023Param, monthSince2023Param,
                dayOfWeekParam, calendarMonthParam, calendarYearParam,
                calendarYearMonthParam, dayOfMonthParam, dayOfYearParam,
                weekOfYearSundayParam, yearWeekSundayParam, weekSundayStartParam,
                weekOfYearMondayParam, yearWeekMondayParam, weekMondayStartParam,
                holidayParam, dayTypeParam);
END IF;
END //

DELIMITER ;