--Check for nulls or duplicates in primary key 
-- expectation : no results
--bronze
select 
cst_id,
COUNT(*) 
from bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--silver
select 
cst_id,
COUNT(*) 
from silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--CHECK FOR UNWANTED SPACES 
--if original value is not equal to the same value after trimming it means there are spaces 
--check for uwanted spaces in "string colums" eg cst_firstname , cst_lastname
--Expectation : NO results 

--bronze
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--silver 
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Quality Check 
--check the consistancy of values in low cardinality columns eg cst_gndr, cst_marital_status
-- In our data warehouse , we aim to store clear and meaningful values rather than using abbreviated terms 
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT 
CASE WHEN cst_gndr = 'F'THEN 'Female'
	 WHEN cst_gndr = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr, 
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S'THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status
FROM bronze.crm_cust_info


-------------------------------------------TRANSFORM----------------
--Check Duplicate using Window fucntions 
-- ROW_NUMBER FUNCTION  
SELECT 
* 
FROM (
	SELECT 
	* ,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
FROM bronze.crm_cust_info ) t WHERE flag_last = 1 

--and cst_id = 29483
--WHERE cst_id = 29466
----------------------------------------------------------------------------------------------------------
---version 2 
--Check Duplicate  and unwanted spaces 
--using Window fucntions  

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname ,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM (
	SELECT 
	* ,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
	FROM bronze.crm_cust_info 
	WHERE cst_id IS NOT NULL
	) t WHERE flag_last = 1 

---------------------------
--VERSION 3
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname ,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S'THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F'THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr ))= 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT 
	* ,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
	FROM bronze.crm_cust_info 
	WHERE cst_id IS NOT NULL
	) t WHERE flag_last = 1 

-------------------------------------------------------
--INSERT NORMALIZED AND STARDARDIZED DATA 
INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname ,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S'THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F'THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr ))= 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT 
	* ,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
	FROM bronze.crm_cust_info 
	WHERE cst_id IS NOT NULL
	) t WHERE flag_last = 1 

	--CHECK NORMALIZED AND STARDARDIZED DATA
	--select * from silver.crm_cust_info
