--CHECK FOR UNWANTED SPACES 
--if original value is not equal to the same value after trimming it means there are spaces 
--check for uwanted spaces in "string colums" eg cst_firstname , cst_lastname
--Expectation : NO results 

--bronze
SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id != TRIM(sls_cust_id)


---check intergrity from in the silver layer crm_prd_info
select 
sls_prd_key
from  bronze.crm_sales_details
where sls_prd_key NOT IN (select prd_key from silver.crm_prd_info)

select 
sls_cust_id
from  bronze.crm_sales_details
where sls_cust_id NOT IN (select cst_id from silver.crm_cust_info)

select * from silver.crm_cust_info

-----Check for invalid dates 
select * from bronze.crm_sales_details

--check for negetive or zeros in the date as they cant cast to date 
--DATE lengh must be 8
select 
NULLIF(sls_order_dt,0) AS sls_order_dt
from  bronze.crm_sales_details
where sls_order_dt <= 0 
or LEN(sls_order_dt) != 8
or sls_order_dt  > 20500101
or sls_order_dt  < 19000101

--cHECK For invalid DATE ORDERS
SELECT * 
FROM  bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt 

---Business Rules 
-- saless = q * price
--negative, zeros, nulls are NOT ALLOWED 

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price) 
	ELSE sls_sales
END sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END sls_price
FROM  bronze.crm_sales_details
WHERE sls_sales !=  sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

--IF sales is negative, zeros or nulls , derive is using Q and price
--if Price is  zero or null calulate using sales and quantity
--IF Price is negatie , convert it to a positive value


IF OBJECT_ID ('silver.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO


-------
INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR  LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR  LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR  LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price) 
	ELSE sls_sales
END sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END sls_price
from bronze.crm_sales_details


--SELECT * FROM silver.crm_sales_details

