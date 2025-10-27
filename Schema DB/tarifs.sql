USE telecom_db
GO

CREATE TABLE tarifs(

	tarif_id INT IDENTITY(1,1) PRIMARY KEY,
	record_type VARCHAR(6),
	technology VARCHAR(5),
	unit VARCHAR(5),
	price DECIMAL(5,4)

);
GO

INSERT INTO tarifs (record_type,technology,unit,price) VALUES
('voice','2G','min','1.0'),
('voice','3G','min','1.1'),
('voice','4G','min','1.2'),
('voice','5G','min','1.3'),
('sms','2G','1','0.9'),
('sms','3G','1','1.0'),
('sms','4G','1','1.2'),
('sms','5G','1','1.4');
GO