# METRO_The Biggest Superstores Chains - Data Warehouse Implementation

## Overview
This project implements a relational data warehouse for the biggest superstores chains in Pakistan, utilizing a Mesh Join algorithm for efficient data processing and integration. The system handles transaction data and product information, loading it into a structured data warehouse for business intelligence purposes.

## Features
- Interactive database authentication with username/password prompt
- Efficient data processing using Mesh Join algorithm
- Batch processing of transaction and master data
- Full dimension and fact table management
- Transaction-safe data loading with commit/rollback support
- Automated data warehouse population
- Error handling and data validation

## Technical Architecture
### Source Databases
- `transactions`: Contains enriched transaction data
- `products_data`: Contains product master data
- `METRO_DW`: Target data warehouse

### Dimension Tables
- Product (PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE)
- Customer (CUSTOMER_ID, CUSTOMER_NAME)
- Supplier (SUPPLIER_ID, SUPPLIER_NAME)
- Store (STORE_ID, STORE_NAME)
- Order Data (ORDER_ID, ORDER_DATE)

### Fact Table
- Sales transactions with:
  - Sales amount
  - Product ID
  - Supplier ID
  - Date
  - Store ID
  - Customer ID
  - Order ID
  - Quantity
  - Total sale

## Dependencies
- MySQL Connector/J (JDBC driver) version 8.2.0
- MySQL Server (local installation)


## Implementation Details
### Batch Processing
- Transaction batch size: 50 records per iteration
- Master data batch size: 1000 records per iteration


## Required Libraries
```java
java.sql.*;
java.sql.Date;
java.time.LocalDate;
java.time.LocalDateTime;
java.time.format.DateTimeFormatter;
java.util.*;