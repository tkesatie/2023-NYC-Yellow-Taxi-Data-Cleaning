-- SQL Script: NYC Yellow Taxi Trip Data Cleaning
-- Project: Data Cleaning Portfolio Project
-- Author: Timothy Kesatie
-- Date: June 26, 2025
-- Description: This script performs initial setup, data loading, exploratory analysis,
--              and a series of data cleaning operations on a subset of the
--              2023 NYC Yellow Taxi Trip Data (Dec 1-7, 2023).
--              It includes steps to handle missing values, correct illogical entries,
--              identify outliers, and create derived columns for further analysis.

-- ====================================================================
-- Section 0: Database Setup and Data Import
-- ====================================================================

-- Use the dedicated schema for this project
USE taxi_data_cleaning;

-- Drop table if it already exists to ensure a clean start for re-running the script
DROP TABLE IF EXISTS taxi_data;

-- Create the taxi_data table with appropriate column definitions
-- Column types are set based on the data dictionary and initial assessment.
CREATE TABLE taxi_data (
    VendorID INT,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INT,
    trip_distance DECIMAL(10, 2),
    RatecodeID INT,
    store_and_fwd_flag VARCHAR(1),
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DECIMAL(10, 2),
    extra DECIMAL(10, 2),
    mta_tax DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2),
    tolls_amount DECIMAL(10, 2),
    improvement_surcharge DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    congestion_surcharge DECIMAL(10, 2),
    airport_fee DECIMAL(10, 2)
);

-- Truncate the table before loading to ensure no duplicate data on re-run
TRUNCATE TABLE taxi_data_cleaning.taxi_data;

-- Loaded data from the CSV file into the taxi_data table.
-- Handles date/time format conversion and converts blank strings to NULL for numerical columns.
LOAD DATA LOCAL INFILE 'C:/Users/meldi/Documents/Portfolio Projects/Yellow Taxi Trip Data Cleaning/2023_Yellow_Taxi_Trip_Data.csv'
INTO TABLE taxi_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @VendorID,
    @tpep_pickup_datetime,
    @tpep_dropoff_datetime,
    @passenger_count,
    @trip_distance,
    @RatecodeID,
    @store_and_fwd_flag,
    @PULocationID,
    @DOLocationID,
    @payment_type,
    @fare_amount,
    @extra,
    @mta_tax,
    @tip_amount,
    @tolls_amount,
    @improvement_surcharge,
    @total_amount,
    @congestion_surcharge,
    @airport_fee
)
SET
    VendorID = @VendorID,
    tpep_pickup_datetime = STR_TO_DATE(@tpep_pickup_datetime, '%m/%d/%Y %h:%i:%s %p'),
    tpep_dropoff_datetime = STR_TO_DATE(@tpep_dropoff_datetime, '%m/%d/%Y %h:%i:%s %p'),
    passenger_count = NULLIF(@passenger_count, ''),
    trip_distance = @trip_distance,
    RatecodeID = NULLIF(@RatecodeID, ''),
    store_and_fwd_flag = @store_and_fwd_flag,
    PULocationID = @PULocationID,
    DOLocationID = @DOLocationID,
    payment_type = @payment_type,
    fare_amount = @fare_amount,
    extra = @extra,
    mta_tax = @mta_tax,
    tip_amount = @tip_amount,
    tolls_amount = @tolls_amount,
    improvement_surcharge = @improvement_surcharge,
    total_amount = @total_amount,
    congestion_surcharge = NULLIF(@congestion_surcharge, ''),
    airport_fee = NULLIF(@airport_fee, '');

-- Verify the number of records loaded
SELECT
    COUNT(*) AS RecordsLoaded
FROM
    taxi_data_cleaning.taxi_data;

-- Create a copy of the original table for data cleaning operations.
-- This ensures the original loaded data remains untouched for reference.
DROP TABLE IF EXISTS taxi_data_copy;
CREATE TABLE taxi_data_copy LIKE taxi_data;
INSERT INTO taxi_data_copy
SELECT
    *
FROM
    taxi_data;

-- ====================================================================
-- Section 1: Initial Data Exploration (Pre-Cleaning Checks)
-- ====================================================================

-- Note: These queries are for initial assessment and understanding data distribution.
-- They help identify anomalies before applying cleaning rules.

-- 1.1 Categorical Data Exploration
SELECT
    *
FROM
    taxi_data_copy
LIMIT 10; -- View sample rows of the working copy

SELECT DISTINCT
    VendorID
FROM
    taxi_data_copy;

SELECT
    VendorID,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
GROUP BY
    VendorID;

SELECT
    passenger_count,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
GROUP BY
    passenger_count;

SELECT
    RatecodeID,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
GROUP BY
    RatecodeID;

SELECT
    RatecodeID,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
WHERE
    passenger_count IS NULL
GROUP BY
    RatecodeID;

SELECT
    store_and_fwd_flag,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
GROUP BY
    store_and_fwd_flag;

SELECT
    DOLocationID,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
GROUP BY
    DOLocationID
ORDER BY
    COUNT(*) DESC;

SELECT
    payment_type,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
GROUP BY
    payment_type;

SELECT
    *
FROM
    taxi_data_copy
WHERE
    payment_type = 0
LIMIT 10; -- Sample Flex Fair Trips

-- 1.2 Numerical Data Exploration
SELECT
    trip_distance,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
GROUP BY
    trip_distance
ORDER BY
    trip_distance;

SELECT
    COUNT(*) AS ZeroTripDistanceCount
FROM
    taxi_data_copy
WHERE
    trip_distance = 0.00;

SELECT
    payment_type,
    COUNT(*) AS RecordCount
FROM
    taxi_data_copy
WHERE
    trip_distance = 0.00
GROUP BY
    payment_type;

SELECT
    MIN(fare_amount) AS MinFare,
    MAX(fare_amount) AS MaxFare
FROM
    taxi_data_copy;

SELECT
    VendorID,
    COUNT(*) AS NegativeFareCount
FROM
    taxi_data_copy
WHERE
    fare_amount < 0
GROUP BY
    VendorID;

SELECT
    COUNT(*) AS OtherNegativeMonetaryValues
FROM
    taxi_data_copy
WHERE
    fare_amount >= 0
    AND (
        extra < 0 OR mta_tax < 0 OR tip_amount < 0 OR tolls_amount < 0 OR
        improvement_surcharge < 0 OR total_amount < 0 OR congestion_surcharge < 0 OR airport_fee < 0
    );

SELECT
    MIN(extra) AS MinExtra,
    MAX(extra) AS MaxExtra
FROM
    taxi_data_copy
WHERE
    extra >= 0;

SELECT
    MIN(mta_tax) AS MinMtaTax,
    MAX(mta_tax) AS MaxMtaTax
FROM
    taxi_data_copy
WHERE
    mta_tax >= 0;

SELECT
    MIN(tip_amount) AS MinTip,
    MAX(tip_amount) AS MaxTip
FROM
    taxi_data_copy
WHERE
    tip_amount >= 0;

SELECT
    MIN(tolls_amount) AS MinTolls,
    MAX(tolls_amount) AS MaxTolls
FROM
    taxi_data_copy
WHERE
    tolls_amount >= 0;

SELECT
    MIN(improvement_surcharge) AS MinImprovementSurcharge,
    MAX(improvement_surcharge) AS MaxImprovementSurcharge
FROM
    taxi_data_copy
WHERE
    improvement_surcharge >= 0;

SELECT
    MIN(congestion_surcharge) AS MinCongestionSurcharge,
    MAX(congestion_surcharge) AS MaxCongestionSurcharge
FROM
    taxi_data_copy
WHERE
    congestion_surcharge >= 0;

SELECT
    MIN(airport_fee) AS MinAirportFee,
    MAX(airport_fee) AS MaxAirportFee
FROM
    taxi_data_copy
WHERE
    airport_fee >= 0;

-- ====================================================================
-- Section 2: Data Cleaning Operations (Deletion/Correction)
-- ====================================================================

-- 2.1. Remove records with undefined VendorID (VendorID = 6)
-- Rationale: These records are from an unknown source and cannot be reliably analyzed.
DELETE FROM
    taxi_data_copy
WHERE
    VendorID = 6;

-- 2.2. Remove records with 0 passenger count
-- Rationale: For a "trip" dataset, 0 passengers indicates an error or non-standard entry.
DELETE FROM
    taxi_data_copy
WHERE
    passenger_count = 0;

-- 2.3. Remove records with negative monetary values
-- Rationale: Monetary values (fares, tips, tolls, etc.) cannot be negative in a real-world scenario.
DELETE FROM
    taxi_data_copy
WHERE
    fare_amount < 0
    OR extra < 0
    OR mta_tax < 0
    OR tip_amount < 0
    OR tolls_amount < 0
    OR improvement_surcharge < 0
    OR total_amount < 0
    OR congestion_surcharge < 0
    OR airport_fee < 0;

-- 2.4. Remove records with invalid time order (dropoff time before or at pickup time)
-- Rationale: A trip cannot logically end before or at the same time it began.
DELETE FROM
    taxi_data_copy
WHERE
    tpep_dropoff_datetime <= tpep_pickup_datetime;

-- ====================================================================
-- Section 3: Creating Derived Columns and Further Anomaly Detection (Flagging)
-- ====================================================================

-- 3.1. Add 'trip_duration_minutes' as a Derived Column
-- Rationale: This is essential for time-based analysis and for applying duration-based cleaning rules.
ALTER TABLE taxi_data_copy
ADD COLUMN trip_duration_minutes INT;
UPDATE taxi_data_copy
SET
    trip_duration_minutes = TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime);

-- 3.2. Remove records with excessively long durations (e.g., > 360 minutes / 6 hours)
-- Rationale: Trips lasting this long are highly improbable for a single, continuous NYC taxi journey and likely indicate data errors.
DELETE FROM
    taxi_data_copy
WHERE
    trip_duration_minutes > 360;

-- 3.3. Add 'average_speed_mph' as a Derived Column
-- Rationale: Helps identify further implausible trips based on calculated speed (e.g., impossibly high speeds).
ALTER TABLE taxi_data_copy
ADD COLUMN average_speed_mph DECIMAL(10, 2);
UPDATE taxi_data_copy
SET
    average_speed_mph = (trip_distance / trip_duration_minutes) * 60
WHERE
    trip_duration_minutes > 0; -- Avoid division by zero for zero-duration trips

-- 3.4. Remove records with unlikely high speeds (e.g., > 45 MPH)
-- Rationale: Speeds exceeding realistic limits for NYC taxi travel indicate data errors. (Researched average NYC speeds to determine threshold).
SELECT
    COUNT(*) AS HighSpeedCount
FROM
    taxi_data_copy
WHERE
    average_speed_mph > 45; -- Check before deleting
DELETE FROM
    taxi_data_copy
WHERE
    average_speed_mph > 45;

-- 3.5. Financial Consistency Check: Flagging 'total_amount' discrepancies
-- Rationale: 'total_amount' should accurately reflect the sum of its components.
--           Large discrepancies indicate data inconsistencies. Flagging allows for later analysis instead of outright deletion.
ALTER TABLE taxi_data_copy
ADD COLUMN is_total_amount_discrepant BOOLEAN DEFAULT FALSE;
UPDATE taxi_data_copy
SET
    is_total_amount_discrepant = TRUE
WHERE
    ABS(
        total_amount - (
            fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge
            -- Note: congestion_surcharge and airport_fee might not be consistently included
            -- in total_amount based on prior analysis. Excluded from this specific sum for now.
        )
    ) > 0.01; -- Using 0.01 as a small tolerance for floating point arithmetic

-- Query to verify the count of flagged discrepancies
SELECT
    COUNT(*) AS FlaggedDiscrepancyCount
FROM
    taxi_data_copy
WHERE
    is_total_amount_discrepant = TRUE;

-- 3.6. Tip Percentage Analysis and Flagging
-- Rationale: High tip percentages (e.g., > 100%) can indicate data errors or unusual scenarios.
--            Flagging allows keeping the data while excluding from tip-specific analysis if needed.
ALTER TABLE taxi_data_copy
ADD COLUMN tip_percentage DECIMAL(10, 2);
UPDATE taxi_data_copy
SET
    tip_percentage = (tip_amount / fare_amount) * 100
WHERE
    fare_amount > 0; -- Avoid division by zero

-- Check for records with tip_percentage > 100%
SELECT
    COUNT(*) AS HighTipPercentageCount,
    MIN(tip_percentage) AS MinHighTip,
    MAX(tip_percentage) AS MaxHighTip
FROM
    taxi_data_copy
WHERE
    tip_percentage > 100;

-- Sample of high tip percentage rows for further inspection
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    fare_amount,
    tip_amount,
    total_amount,
    tip_percentage
FROM
    taxi_data_copy
WHERE
    tip_percentage > 100
ORDER BY
    tip_percentage DESC
LIMIT 10;

-- Add a flag column for easy filtering later.
ALTER TABLE taxi_data_copy
ADD COLUMN is_high_tip_outlier BOOLEAN DEFAULT FALSE;
UPDATE taxi_data_copy
SET
    is_high_tip_outlier = TRUE
WHERE
    tip_percentage > 100;


-- 3.7. Tolls Amount Analysis and Flagging
-- Rationale: Unusually high tolls amounts can indicate errors or very specific long-distance trips.
--            Flagging allows for conditional exclusion in analysis.
-- Check for records with very high tolls_amount (e.g., > $50, adjust as per NYC toll context)
SELECT
    COUNT(*) AS HighTollsCount,
    MIN(tolls_amount) AS MinHighTolls,
    MAX(tolls_amount) AS MaxTolls
FROM
    taxi_data_copy
WHERE
    tolls_amount > 50;

-- Sample of high tolls amount rows for further inspection
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount,
    tolls_amount,
    total_amount
FROM
    taxi_data_copy
WHERE
    tolls_amount > 50
ORDER BY
    tolls_amount DESC
LIMIT 10;

-- Add a flag column for easy filtering later.
ALTER TABLE taxi_data_copy
ADD COLUMN is_high_toll_outlier BOOLEAN DEFAULT FALSE;
UPDATE taxi_data_copy
SET
    is_high_toll_outlier = TRUE
WHERE
    tolls_amount > 50;

-- ====================================================================
-- Section 4: Final Verification and Summary (Post-Cleaning Overview)
-- ====================================================================

-- Query to get the final record count after all deletions and cleaning operations.
SELECT
    COUNT(*) AS FinalRecordCount
FROM
    taxi_data_copy;

-- Query to get summary statistics of key numerical columns after cleaning.
-- This provides a high-level overview of the cleaned dataset's characteristics.
SELECT
    COUNT(*) AS TotalRecords,
    MIN(trip_distance) AS MinDistance,
    MAX(trip_distance) AS MaxDistance,
    AVG(trip_distance) AS AvgDistance,
    MIN(fare_amount) AS MinFare,
    MAX(fare_amount) AS MaxFare,
    AVG(fare_amount) AS AvgFare,
    MIN(total_amount) AS MinTotal,
    MAX(total_amount) AS MaxTotal,
    AVG(total_amount) AS AvgTotal,
    MIN(trip_duration_minutes) AS MinDuration,
    MAX(trip_duration_minutes) AS MaxDuration,
    AVG(trip_duration_minutes) AS AvgDuration,
    MIN(average_speed_mph) AS MinSpeed,
    MAX(average_speed_mph) AS MaxSpeed,
    AVG(average_speed_mph) AS AvgSpeed
FROM
    taxi_data_copy;
