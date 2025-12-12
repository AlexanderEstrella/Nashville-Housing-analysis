----------------------------------------------------------------------------------------------------
-- Format SaleDate for presentation (do NOT overwrite the original date column)

-- Quick sanity check of raw data
SELECT * 
FROM NashvilleHousing;

-- Example of formatting a date (used only for display, not storage)
SELECT FORMAT(SaleDate, 'MM/dd/yyyy')
FROM NashvilleHousing;

-- Add a text-based date column for formatted output
ALTER TABLE NashvilleHousing 
ADD SaleDateText varchar(10);

-- Populate formatted date column
UPDATE NashvilleHousing
SET SaleDateText = FORMAT(SaleDate, 'MM/dd/yyyy');
----------------------------------------------------------------------------------------------------

-- Populate missing PropertyAddress values using other rows with the same ParcelID
-- Assumption: Same ParcelID = same physical property

UPDATE a
SET a.PropertyAddress = COALESCE(a.PropertyAddress, b.PropertyAddress)
FROM NashvilleHousing a
JOIN NashvilleHousing b 
  ON a.ParcelID = b.ParcelID 
 AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.PropertyAddress IS NULL;

-- SELECT * FROM NashvilleHousing; -- sanity check
----------------------------------------------------------------------------------------------------

-- Break PropertyAddress into individual components (StreetNumber, Road/Designator, City)
-- Notes on functions used:
-- SUBSTRING(string, start_index, number_of_characters)
-- LEN(string) returns string length
-- LEFT(string, n) returns first n characters
-- CHARINDEX(char, string) returns position of first occurrence (0 if not found)

SELECT
  TRIM(LEFT(PropertyAddress, CHARINDEX(' ', PropertyAddress) - 1)) AS StreetNumber,
  TRIM(
      SUBSTRING(
          TRIM(PARSENAME(REPLACE(PropertyAddress, ',', '.'), 2)),
          CHARINDEX(' ', TRIM(PropertyAddress)),
          LEN(PropertyAddress)
      )
  ) AS Designator,
  TRIM(PARSENAME(REPLACE(PropertyAddress, ',', '.'), 1)) AS City
FROM NashvilleHousing;

-- Normalize PropertyAddress formatting by trimming extra spaces
UPDATE NashvilleHousing
SET PropertyAddress = TRIM(PropertyAddress);

----------------------------------------------------------------------------------------------------

-- Normalize SoldAsVacant values for consistency
-- Convert Yes/No to True/False, mark unexpected values as Unknown

UPDATE NashvilleHousing
SET SoldAsVacant = CASE
    WHEN SoldAsVacant = 'No'  THEN 'False'
    WHEN SoldAsVacant = 'Yes' THEN 'True'
    ELSE 'Unknown'
END;

--------------------------------------------------------------------------------------------

-- Remove duplicate records
-- Duplicate definition: same ParcelID, PropertyAddress, SaleDate, and LegalReference
-- Keep the most recent record (highest UniqueID)

WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ParcelID, PropertyAddress, SaleDate, LegalReference
               ORDER BY UniqueID DESC
           ) AS row_count
    FROM NashvilleHousing
)
DELETE
FROM cte
WHERE row_count > 1;

-- Validation query (used before delete)
-- SELECT *, ROW_NUMBER() OVER (PARTITION BY ParcelID, PropertyAddress, SaleDate, LegalReference ORDER BY UniqueID DESC) AS row_count
-- FROM NashvilleHousing;

----------------------------------------------------------------------------------------------------

-- Remove unused columns after analysis is complete
ALTER TABLE NashvilleHousing
DROP COLUMN OwnerAddress, TaxDistrict;

----------------------------------------------------------------------------------------------------

-- Identify parcels with more than one owner record
SELECT ParcelID, COUNT(OwnerName) AS OwnerCount
FROM NashvilleHousing
GROUP BY ParcelID
HAVING COUNT(OwnerName) > 1;

-- Identify cases where one row is missing OwnerAddress but another row for same ParcelID has it
SELECT *
FROM NashvilleHousing a
JOIN NashvilleHousing b 
  ON a.ParcelID = b.ParcelID 
 AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.OwnerAddress IS NULL
  AND b.OwnerAddress IS NOT NULL;

-- Window-function approach to find parcels that have an address somewhere,
-- but this specific row is missing it
SELECT *
FROM (
    SELECT a.*,
           MAX(CASE WHEN a.PropertyAddress IS NOT NULL THEN 1 ELSE 0 END)
             OVER (PARTITION BY a.ParcelID) AS ParcelHasAddress
    FROM NashvilleHousing a
) x
WHERE x.PropertyAddress IS NULL
  AND x.ParcelHasAddress = 1;

-- Inspect a specific ParcelID for validation
SELECT *
FROM NashvilleHousing
WHERE ParcelID = '061 15 0 043.00';

------------------------------------------------------------------------------

-- High-level value sanity checks (outlier awareness)
SELECT
    MAX(TotalValue)     AS MaxTotalValue,
    MAX(LandValue)      AS MaxLandValue,
    MAX(BuildingValue)  AS MaxBuildingValue,
    MAX(Bedrooms)       AS MaxBedrooms,
    MAX(FullBath)       AS MaxFullBaths
FROM NashvilleHousing;

----------------------------------------------------------------------------------------------------

-- Feature engineering + analytical metrics using CTEs
-- SRC: cleaned, parsed base dataset
-- Avgs: city-level averages and rolling metrics
-- Median: city-level median sale price

;WITH SRC AS (
    SELECT
        TRIM(LEFT(PropertyAddress, CHARINDEX(' ', PropertyAddress) - 1)) AS StreetNumber,
        TRIM(
            SUBSTRING(
                TRIM(PARSENAME(REPLACE(PropertyAddress, ',', '.'), 2)),
                CHARINDEX(' ', TRIM(PARSENAME(REPLACE(PropertyAddress, ',', '.'), 2))) + 1,
                LEN(TRIM(PARSENAME(REPLACE(PropertyAddress, ',', '.'), 2)))
            )
        ) AS Designator,
        TRIM(PARSENAME(REPLACE(PropertyAddress, ',', '.'), 1)) AS City,
        TotalValue,
        SaleDate,
        Acreage,
        LandValue,
        BuildingValue,
        Bedrooms,
        FullBath,
        HalfBath,
        SalePrice
    FROM NashvilleHousing
),
Avgs AS (
    SELECT
        StreetNumber,
        Designator,
        City,
        TotalValue,
        SalePrice,
        SaleDate,
        CAST(AVG(TotalValue) OVER (PARTITION BY City) AS decimal(18,2)) AS AverageTotalValueByCity,
        CAST(AVG(SalePrice)  OVER (PARTITION BY City) AS decimal(18,2)) AS AverageSalePriceByCity,
        SUM(SalePrice) OVER (
            ORDER BY SaleDate
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS Rolling30DaySumSalePrice
    FROM SRC
    WHERE TotalValue IS NOT NULL
),
Median AS (
    SELECT DISTINCT
        City,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY SalePrice)
            OVER (PARTITION BY City) AS MedianSalePrice
    FROM SRC
    WHERE SalePrice IS NOT NULL
)
SELECT
    a.City,
    a.AverageTotalValueByCity,
    a.AverageSalePriceByCity,
    a.Rolling30DaySumSalePrice,
    m.MedianSalePrice
FROM Avgs a
JOIN Median m
  ON m.City = a.City;
