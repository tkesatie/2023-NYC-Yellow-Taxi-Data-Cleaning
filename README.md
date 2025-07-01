1\. Project Overview
--------------------

This project dives into the critical first step of any data analysis: cleaning and preparing raw data. I've taken a subset of the 2023 NYC Yellow Taxi Trip Data and applied a systematic approach to identify and resolve various data quality issues. My goal was to transform a raw, messy dataset into a reliable and consistent foundation ready for in-depth analytical tasks. This process involved a blend of initial data exploration, decisive cleaning (removing truly problematic records), and intelligent flagging of anomalies that warrant further investigation without being outright discarded.

2\. Data Source
---------------

-   **Dataset:** 2023 Yellow Taxi Trip Data

-   **Source:** NYC Open Data

-   **Link:**  <https://data.cityofnewyork.us/Transportation/2023-Yellow-Taxi-Trip-Data/4b4i-vvec/about_data>

-   I heavily relied on the provided data dictionary as my primary reference throughout the project.

-   **Data Subset:** To keep the project manageable, I focused on a specific week: **December 1, 2023, to December 7, 2023**. This subset still provided a substantial amount of data, totaling **845,959 records**.

3\. Tools Used
--------------

-   **Database:** MySQL (My choice for its robust capabilities in handling large datasets and complex data manipulation).

-   **Scripting Language:** SQL

4\. Data Ingestion and Setup
----------------------------

My initial setup involved:

-   Creating a dedicated MySQL schema, `taxi_data_cleaning`.

-   Defining the `taxi_data` table, mirroring the original dataset's column structure.

-   Loading the raw CSV data into `taxi_data` using `LOAD DATA LOCAL INFILE`. A crucial step here was explicitly converting blank values in numerical columns (`passenger_count`, `RatecodeID`, `congestion_surcharge`, `airport_fee`) to `NULL` during import to prevent data type errors.

-   Finally, I created `taxi_data_copy` as my working table, ensuring the original imported data remained untouched for reference. All subsequent cleaning operations were performed on this copy.

5\. My Data Cleaning Process
----------------------------

My cleaning process was an iterative cycle of exploration, identifying issues, and then applying targeted rules.

### Initial Data Exploration

Through initial checks and queries, I uncovered several data quality issues:

-   **`VendorID`:** I found an unexpected `VendorID = 6`, which wasn't defined in the data dictionary. These records clearly came from an unknown source.

-   **`passenger_count`:** I noticed a significant number of `NULL` and `0` values. While `0` passengers often indicated an error for a standard trip, my investigation revealed that some `NULL` values were legitimately tied to specific trip types (like Flex Fair Trips).

-   **`RatecodeID`:** Many `NULL` values appeared here, and I confirmed they directly correlated with the `NULL`  `passenger_count` records.

-   **`store_and_fwd_flag`:** I found blanks (which became `NULL`s after import) in this column, similar in count to other `NULL` issues.

-   **`PULocationID` / `DOLocationID`:** Despite some location IDs having very few records, I didn't find any issues that warranted removing data based on their distribution.

-   **`payment_type`:** A key discovery was `payment_type = 0`, representing "Flex Fair Trips" -- a non-metered trip type. After deeper investigation, I decided to retain these records, even though they had `NULL`s in several other columns (`passenger_count`, `RatecodeID`, etc.), because they provided valuable payment information.

-   **`trip_distance`:** A notable `12,227` records showed `0.00` trip distance. I chose not to remove these outright, as they still contained valid payment information, but I've noted them for careful consideration during analysis.

-   **Monetary Values (`fare_amount`, `extra`, `tip_amount`, `tolls_amount`, etc.):**

    -   **Negative Values:** I identified negative values across various monetary columns, particularly in `fare_amount` from `VendorID = 2`. These were clearly data entry errors and were targeted for removal.

    -   **Outliers:** I observed unusually high `tip_amount` and `tolls_amount` values that didn't seem typical. Instead of removing them, I opted to flag these for detailed inspection, allowing me to decide their fate during specific analyses.

### Applying Cleaning Rules (Deletions)

Based on my initial exploration, I removed the following records from `taxi_data_copy` because they were either illogical or unrecoverable data errors:

-   **Undefined `VendorID`:** Records where `VendorID = 6`.

-   **Zero `passenger_count`:** Records where `passenger_count = 0`. (As noted, `NULL` passenger counts linked to Flex Fair Trips were kept).

-   **Negative Monetary Values:** Any record with a negative value in `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, or `airport_fee`.

-   **Invalid Time Order:** Records where the `tpep_dropoff_datetime` was before or identical to the `tpep_pickup_datetime`.

-   **Excessively Long Trip Durations:** Records where the calculated `trip_duration_minutes` exceeded 360 minutes (6 hours), as these are highly improbable for a single NYC taxi trip.

-   **Unlikely High Speeds:** Records where the calculated `average_speed_mph` was over 45 MPH, an unrealistic speed for typical NYC taxi travel.

### Creating New Features and Anomaly Flags

To enhance the dataset for analysis and provide flexibility in handling remaining anomalies, I added the following derived columns and flags:

-   **`trip_duration_minutes` (INT):** Calculated using `TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)`.

-   **`average_speed_mph` (DECIMAL):** Calculated as `(trip_distance / trip_duration_minutes) * 60` (only for trips with `trip_duration_minutes > 0`).

-   **`is_total_amount_discrepant` (BOOLEAN):** Set to `TRUE` if `total_amount` didn't match the sum of its components (`fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge`) within a $0.01 tolerance. My analysis revealed that `congestion_surcharge` and `airport_fee` were often *not* consistently included in the `total_amount` in the raw data, highlighting a key data inconsistency.

-   **`tip_percentage` (DECIMAL):** Calculated as `(tip_amount / fare_amount) * 100` (only for `fare_amount > 0`).

-   **`is_high_tip_outlier` (BOOLEAN):** Set to `TRUE` if `tip_percentage` exceeded 100%. These records are kept but flagged for conditional exclusion during tip-specific analyses.

-   **`is_high_toll_outlier` (BOOLEAN):** Set to `TRUE` if `tolls_amount` exceeded $50 (a threshold I set based on typical NYC tolls). Similar to high tips, these are retained but flagged for specific analysis contexts.

6\. Summary of Cleaned Data
---------------------------

After applying all these cleaning operations, my `taxi_data_copy` table now represents a significantly more reliable and consistent dataset. A final count and summary statistics of key numerical columns provide a clear overview of the refined data's characteristics, ready for deeper analytical exploration.

7\. Future Work / Next Steps
----------------------------

-   **In-depth Analysis of Flagged Data:** I plan to dive deeper into the records flagged with `is_total_amount_discrepant`, `is_high_tip_outlier`, and `is_high_toll_outlier` to fully understand their root causes and potential impact on various analyses.

-   **Geospatial Analysis:** I'm interested in exploring the `PULocationID` and `DOLocationID` by joining them with a NYC taxi zone shapefile to uncover spatial insights into trip patterns.

-   **Time Series Analysis:** I'll analyze trip patterns based on time, such as hour of day, day of week, and potentially month, using the datetime columns.

-   **Further Feature Engineering:** I'll create additional temporal features like `day_of_week`, `hour_of_day`, and `month` from the datetime stamps.

-   **Hypothesis Testing/Modeling:** With the cleaned data, I aim to test specific hypotheses or build predictive models, perhaps for predicting tip amounts, trip durations, or fares.
