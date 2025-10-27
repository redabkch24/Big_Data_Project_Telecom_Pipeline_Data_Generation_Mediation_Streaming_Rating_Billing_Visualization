USE telecom_db
GO

DELETE FROM billing_cdrs
GO

DROP TABLE IF EXISTS billing_cdrs;
GO

CREATE TABLE billing_cdrs(

	cdr_id INT IDENTITY(1,1) PRIMARY KEY,
	user_id VARCHAR(13),
	Month INT,
	nb_sms INT,
	total_duration_sec INT,
	total_amount DECIMAL(10,4)

);