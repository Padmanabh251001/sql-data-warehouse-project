/*
===================================================================================================

Create Database and Schemas

=======================================================================================================

Script purpose:
	This script creates a new database called "DataWarehouse" after checking if it already exists.
	If the database exisits, it is dropped and recreated. Additionally the script sets up three schemas
	within the database: "bronze","silver","gold".

Warning:
	Running this script will drop the entire "DataWarehouse" database if it exists.
	All data in the database will be permanently deleted. Proceed with caution 
	and ensure you you have proper backups before running this script.
*/

use master;
go

-- drop and recreate the 'DataWarehouse' database
if exists(select 1 from sys.databases where name = 'DataWareHouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
go

-- database creation
create database DataWarehouse;
go

-- using new created database (DataWarehouse)
use DataWarehouse;
go 

--creating schemas (bronze,silver,gold)
create schema bronze;
go

create schema silver;
go 

create schema gold;
