CREATE DATABASE `METRO_DW`;
USE `METRO_DW`;

-- Product Dimension Table
CREATE TABLE ProductDimension (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    ProductPrice DOUBLE
);

-- Store Dimension Table
CREATE TABLE StoreDimension (
    StoreID INT PRIMARY KEY,
    StoreName VARCHAR(255),
    Location VARCHAR(255)
);

-- Customer Dimension Table
CREATE TABLE CustomerDimension (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Gender VARCHAR(10),
    Age INT
);

-- Supplier Dimension Table
CREATE TABLE SupplierDimension (
    SupplierID INT PRIMARY KEY,
    SupplierName VARCHAR(255),
    ContactInfo VARCHAR(255)
);

-- Time Dimension Table
CREATE TABLE TimeDimension (
    timeID INT PRIMARY KEY AUTO_INCREMENT,
    day INT,
    month INT,
    year INT,
    datee DATE
);

-- Fact Table
CREATE TABLE SalesFact (
    FactID INT PRIMARY KEY AUTO_INCREMENT,
    OrderID INT,
    ProductID INT,
    timeID INT,
    CustomerID INT,
    StoreID INT,
    SupplierID INT,
    QuantityOrdered INT,
    ProductPrice DECIMAL(10, 2),
    Revenue DECIMAL(10, 2) AS (QuantityOrdered * ProductPrice),
    FOREIGN KEY (ProductID) REFERENCES ProductDimension(ProductID),
    FOREIGN KEY (timeID) REFERENCES TimeDimension(timeID),
    FOREIGN KEY (CustomerID) REFERENCES CustomerDimension(CustomerID),
    FOREIGN KEY (StoreID) REFERENCES StoreDimension(StoreID),
    FOREIGN KEY (SupplierID) REFERENCES SupplierDimension(SupplierID)
);

