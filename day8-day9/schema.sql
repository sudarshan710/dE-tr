DROP TABLE IF EXISTS FactReturns, FactSales,
                    DimCustomer, DimProduct, DimDate, DimStore, DimRegion;

CREATE TABLE DimCustomer (
    CustomerSK INT AUTO_INCREMENT PRIMARY KEY,
    CustomerID INT NOT NULL,
    Name VARCHAR(100),
    Address VARCHAR(255),
    LoyaltyTier VARCHAR(50),
    StartDate DATE NOT NULL,
    EndDate DATE,
    IsCurrent BOOLEAN DEFAULT TRUE,
    UNIQUE (CustomerID, StartDate)
);

CREATE TABLE DimProduct (
    ProductSK INT AUTO_INCREMENT PRIMARY KEY,
    ProductID INT NOT NULL UNIQUE,
    Name VARCHAR(100),
    Category VARCHAR(50),
    Brand VARCHAR(50)
);

CREATE TABLE DimDate (
    DateSK INT PRIMARY KEY,
    Date DATE NOT NULL UNIQUE,
    Year INT,
    Month INT,
    DayOfWeek INT,
    Quarter INT
);

CREATE TABLE DimRegion (
    RegionSK INT AUTO_INCREMENT PRIMARY KEY,
    RegionID INT NOT NULL UNIQUE,
    RegionName VARCHAR(100)
);

CREATE TABLE DimStore (
    StoreSK INT AUTO_INCREMENT PRIMARY KEY,
    StoreID INT NOT NULL UNIQUE,
    Name VARCHAR(100),
    City VARCHAR(50),
    RegionSK INT,
    FOREIGN KEY (RegionSK) REFERENCES DimRegion(RegionSK)
);

CREATE TABLE FactSales (
    SalesID INT AUTO_INCREMENT PRIMARY KEY,
    DateSK INT NOT NULL,
    CustomerSK INT NOT NULL,
    ProductSK INT NOT NULL,
    StoreSK INT NOT NULL,
    Quantity INT,
    Revenue DECIMAL(10, 2),
    FOREIGN KEY (DateSK) REFERENCES DimDate(DateSK),
    FOREIGN KEY (CustomerSK) REFERENCES DimCustomer(CustomerSK),
    FOREIGN KEY (ProductSK) REFERENCES DimProduct(ProductSK),
    FOREIGN KEY (StoreSK) REFERENCES DimStore(StoreSK)
);

CREATE TABLE FactReturns (
    ReturnID INT AUTO_INCREMENT PRIMARY KEY,
    DateSK INT NOT NULL,
    CustomerSK INT NOT NULL,
    ProductSK INT NOT NULL,
    StoreSK INT NOT NULL,
    Reason VARCHAR(100),
    ReturnAmount DECIMAL(10, 2),
    FOREIGN KEY (DateSK) REFERENCES DimDate(DateSK),
    FOREIGN KEY (CustomerSK) REFERENCES DimCustomer(CustomerSK),
    FOREIGN KEY (ProductSK) REFERENCES DimProduct(ProductSK),
    FOREIGN KEY (StoreSK) REFERENCES DimStore(StoreSK)
);