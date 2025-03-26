-- Create database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TaxiTripsDb')
BEGIN
    CREATE DATABASE TaxiTripsDb;
    PRINT 'Database TaxiTripsDb created.';
END
ELSE
BEGIN
    PRINT 'Database TaxiTripsDb already exists.';
END
GO

-- Optionally switch to it
USE TaxiTripsDb;
GO

CREATE TABLE TaxiTrips (
    Id BIGINT IDENTITY PRIMARY KEY,
    PickupDateTime DATETIME NOT NULL,
    DropoffDateTime DATETIME NOT NULL,
    PassengerCount INT,
    TripDistance DECIMAL(10,2),
    StoreAndFwdFlag NVARCHAR(3),
    PULocationID INT,
    DOLocationID INT,
    FareAmount DECIMAL(10,2),
    TipAmount DECIMAL(10,2)
);


CREATE INDEX IX_TaxiTrips_PULocationID_TipAmount
ON TaxiTrips (PULocationID, TipAmount);

CREATE INDEX IX_TaxiTrips_TripDistance
ON TaxiTrips (TripDistance DESC);

CREATE INDEX IX_TaxiTrips_Pickup_Dropoff
ON TaxiTrips (PickupDateTime, DropoffDateTime);

CREATE INDEX IX_TaxiTrips_PULocationID
ON TaxiTrips (PULocationID);
