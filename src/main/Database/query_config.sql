USE config;
DELIMITER
//

CREATE PROCEDURE GetConfigId(IN configName VARCHAR (255), OUT configId INT)
BEGIN
SELECT id
INTO configId
FROM config.config
WHERE name = configName;
END
//

DELIMITER ;
DELIMITER
//

CREATE PROCEDURE GetLocationDateDim(IN configId INT, OUT locationDateDim VARCHAR (255))
BEGIN
SELECT location_date_dim
INTO locationDateDim
FROM config.config
WHERE id = configId;
END
//

DELIMITER ;
    DELIMITER
//
DELIMITER //

CREATE PROCEDURE IsStatusSuccess(OUT isSuccess BOOLEAN, IN stepName VARCHAR (255))
BEGIN
SELECT status = 'SUCCESS'
INTO isSuccess
FROM config.config
WHERE flags = 1;

-- Optionally, you can add additional logic or error handling here
END
//

DELIMITER ;

DELIMITER
//

DELIMITER //

CREATE PROCEDURE GetUserName(IN idConfig INT, IN nameIn VARCHAR (255), OUT userName VARCHAR (255))
BEGIN
SELECT user_name
INTO userName
FROM control
WHERE id_config = idConfig
  and name = nameIn;
END
//

DELIMITER ;
DELIMITER
//

CREATE PROCEDURE GetPassword(IN idConfig INT, IN nameIn VARCHAR (255), OUT password VARCHAR (255))
BEGIN
SELECT pass
INTO password
FROM control
WHERE id_config = idConfig
  and name = nameIn;
END
//

DELIMITER ;
DELIMITER
//

CREATE PROCEDURE GetSourcePath(IN id INT, OUT sourcePath VARCHAR (255))
BEGIN
SELECT source_path
INTO sourcePath
FROM config
WHERE id = id;
END
//

DELIMITER ;

    DELIMITER //

CREATE PROCEDURE UpdateStatus(
    IN inTableName VARCHAR(255),
    IN inNewStatus VARCHAR(255)
)
BEGIN
UPDATE config.config
SET status = inNewStatus, update_at = NOW()
WHERE name = inTableName AND flags = 1;
END //

DELIMITER ;
    DELIMITER //

CREATE PROCEDURE GetErrorEmail(IN inIdConfig INT, IN inName VARCHAR(45), OUT outErrorEmail VARCHAR(100))
BEGIN
    -- Declare a variable to store the error_email value
    DECLARE errorEmailVar VARCHAR(100);

    -- Select the error_email value based on id_config and name
SELECT error_email INTO errorEmailVar
FROM config.config
WHERE id= inIdConfig AND name = inName;

-- Set the output parameter
SET outErrorEmail = errorEmailVar;
END //

DELIMITER ;