-- Create databases
CREATE DATABASE IF NOT EXISTS metastore_db;

-- Create admin user and grant privileges
CREATE USER IF NOT EXISTS 'admin'@'%' IDENTIFIED BY 'admin';
GRANT ALL PRIVILEGES ON metastore_db.* TO 'admin'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION;

-- Set character set
ALTER DATABASE metastore_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

FLUSH PRIVILEGES;