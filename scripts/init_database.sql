/*--------------------------------------------------------------------------------

--  PURPOSE : Creates (or fully replaces) the DataWarehouse database and the
              bronze / silver / gold layers used by the data-platform.

--  WARNING  : The script drops any existing database named “DataWarehouse”
               and rebuilds it from scratch. All previous data will be lost.

--  RUN AS   : A login that has CREATE DATABASE permission (e.g. sysadmin).

--------------------------------------------------------------------------------*/

-- 1.  Remove the database if it already exists 
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END
GO

-- 2.  Create a fresh DataWarehouse 
CREATE DATABASE DataWarehouse;
GO

-- 3.  Switch context to the new database 
USE DataWarehouse;
GO

-- 4.  Create the three layer-specific schemas 
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
GO
