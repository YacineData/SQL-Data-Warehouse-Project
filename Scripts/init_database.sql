/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
	This script creates a new database called 'DataWarehouse' after checking for its existence. 
	If the database already exists, it is dropped and recreated. 
	The script also sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Creating the Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

-- Using the Database 
USE DataWarehouse;
GO


-- Creating The Schemas for Bronze/Silver/Gold Layers
CREATE SCHEMA bronze;
GO 
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
