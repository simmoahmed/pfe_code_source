-- Create Person table
CREATE TABLE Person (
  PersonID VARCHAR(255) PRIMARY KEY,
  LastName VARCHAR(255),
  FirstName VARCHAR(255),
  DateOfBirth DATE,
  Email VARCHAR(255),
  PhoneNumber VARCHAR(255),
  Address VARCHAR(255),
  Country VARCHAR(255),
  City VARCHAR(255),
  ZipCode VARCHAR(255),
  SocialSecurityNumber VARCHAR(255),
  MaritalStatus VARCHAR(255)
);

-- Create Employee table
CREATE TABLE Employee (
  EmployeeID VARCHAR(255),
  PersonID VARCHAR(255),
  BranchID VARCHAR(255),
  AccountID VARCHAR(255),
  Position VARCHAR(255),
  PRIMARY KEY (EmployeeID),
  FOREIGN KEY (PersonID) REFERENCES Person(PersonID),
  FOREIGN KEY (BranchID) REFERENCES BRANCH(BranchID),
  FOREIGN KEY (AccountID) REFERENCES Account(AccountID)
);

-- Create Customer table
CREATE TABLE Customer (
  CustomerID VARCHAR(255),
  PersonID VARCHAR(255),
  AccountID VARCHAR(255),
  CustomerType VARCHAR(255),
  PRIMARY KEY (CustomerID),
  FOREIGN KEY (PersonID) REFERENCES Person(PersonID),
  FOREIGN KEY (AccountID) REFERENCES Account(AccountID)
);

-- Create Loan table
CREATE TABLE LOAN (
  LoanID INT PRIMARY KEY,
  CustomerID VARCHAR(255),
  LoanType VARCHAR(255),
  LoanAmount FLOAT,
  InterestRate DECIMAL(10, 2),
  OpeningDate DATE,
  LoanTerm_Duration INT,
  StartDate DATE,
  EndDate DATE,
  Status VARCHAR(255),
  FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);

-- Create LoanPayment table
CREATE TABLE LoanPayment (
  LoanPaymentID INT,
  LoanID INT,
  ScheduledPaymentDate DATE,
  PaymentAmount DECIMAL(10, 2),
  PrincipalAmount DECIMAL(10, 2),
  InterestAmount DECIMAL(10, 2),
  PaidAmount DECIMAL(10, 2),
  PainDate DECIMAL(10, 2),
  PRIMARY KEY (LoanPaymentID),
  FOREIGN KEY (LoanID) REFERENCES LOAN(LoanID)
);

-- Create Transaction table
CREATE TABLE Transaction (
  TransactionID INT,
  LoanPaymentID INT,
  AccountID VARCHAR(255),
  EmployeeID VARCHAR(255),
  TransactionType VARCHAR(255),
  Amount DECIMAL(10, 2),
  TransactionDate DATE,
  PRIMARY KEY (TransactionID),
  FOREIGN KEY (LoanPaymentID) REFERENCES LoanPayment(LoanPaymentID),
  FOREIGN KEY (AccountID) REFERENCES Account(AccountID),
  FOREIGN KEY (EmployeeID) REFERENCES Employee(EmployeeID)
);

-- Create BANK_CARD table
CREATE TABLE BANK_CARD (
  CardID VARCHAR(255) PRIMARY KEY,
  PersonID VARCHAR(255),
  CardNumber VARCHAR(255),
  CardType VARCHAR(255),
  PINCode INT,
  ExpirationDate DATE,
  FOREIGN KEY (PersonID) REFERENCES Person(PersonID)
);

-- Create BRANCH table
CREATE TABLE BRANCH (
  BranchID VARCHAR(255) PRIMARY KEY,
  BrancheName VARCHAR(255),
  BranchCode VARCHAR(255),
  Address VARCHAR(255),
  PhoneNumber VARCHAR(255)
);

-- Create Account table
CREATE TABLE Account (
  AccountID VARCHAR(255) PRIMARY KEY,
  AccountType VARCHAR(255),
  RIB INT,
  CodeSWIFT VARCHAR(255),
  CurrentBalance DECIMAL(10, 2),
  OpeningDate DATE,
  AccountStatus VARCHAR(255),
  MonthlyFees FLOAT
);

