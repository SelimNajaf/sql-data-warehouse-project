/*
DESCRIPTION

This script completely resets the three-layer data-warehouse environment
used for medallion architecture (bronze → silver → gold).

- If any of the databases “bronze”, “silver” or “gold” already exist,
  they are **IRREVERSIBLY DROPPED** together with **ALL** objects and data
  that live inside them.
- Afterwards the three schemas are re-created from scratch, empty.

Run this script only when you are absolutely sure you want to destroy
previous work and start over.
*/

--  W A R N I N G
-- --------------------------------------------------------------------------
-- This code **DELETES** existing databases and recreates them from zero.
-- All data, tables, views, functions, permissions, etc. will be lost.
-- Review twice, execute once.
-- --------------------------------------------------------------------------

-- Drop the old environments if they exist
DROP DATABASE IF EXISTS bronze;
DROP DATABASE IF EXISTS silver;
DROP DATABASE IF EXISTS gold;

-- Re-create empty schemas (databases) for the medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
