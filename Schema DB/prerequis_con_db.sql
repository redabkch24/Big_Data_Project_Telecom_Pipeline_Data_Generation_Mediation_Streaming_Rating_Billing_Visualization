USE [master]
GO

CREATE LOGIN wsl_user WITH PASSWORD = '0000';
GO

USE telecom_db
GO

CREATE USER wsl_user FOR LOGIN wsl_user;
ALTER ROLE db_datawriter ADD MEMBER wsl_user;
ALTER ROLE db_datareader ADD MEMBER wsl_user;
GO