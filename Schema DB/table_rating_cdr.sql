USE telecom_db
GO

DELETE FROM rating_cdrs
GO

DROP TABLE IF EXISTS rating_cdrs;
GO

CREATE TABLE rating_cdrs(

	rcdr_id INT IDENTITY(1,1) PRIMARY KEY,
	record_type VARCHAR(6),
	timestmp VARCHAR(20),
	caller_id VARCHAR(13),
	callee_id VARCHAR(13),
	sender_id VARCHAR(13),
	receiver_id VARCHAR(13),
	user_id VARCHAR(13),
	duration_sec INT,
	cell_id VARCHAR(20),
	technology VARCHAR(5),
	cost DECIMAL(6,4),
	tarif_id INT

);