--Check for nulls or duplicates in primary key 
-- expectation : no results
--bronze
select 
prd_id,
COUNT(*) 
from bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL



--silver
select 
cst_id,
COUNT(*) 
from silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


select * from bronze.crm_prd_info


------
--Check for unwanted spaces. eg prd_line , prd_nm 
--Expectation : No Results 

select prd_line
from bronze.crm_prd_info
where prd_line != TRIM(prd_line)

----check for nulls or negative numbers 
--Expection : No results 

select prd_cost
from silver.crm_prd_info
where prd_cost IS NULL OR  prd_cost < 0 

--Quality Check 
--check the consistancy of values in low cardinality columns eg prd_line
-- In our data warehouse , we aim to store clear and meaningful values rather than using abbreviated terms 
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT
CASE WHEN UPPER(TRIM(prd_line)) = 'M 'THEN 'Mountain'
	 WHEN UPPER(TRIM(prd_line)) = 'R'THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	 WHEN UPPER(TRIM(prd_line)) = 'T'THEN 'Touring'
	 ELSE 'n/a'
END prd_line
FROM bronze.crm_prd_info

----check for invalid date orders
select * 
from silver.crm_prd_info
where prd_end_dt <  prd_start_dt 

select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS prd_end_dt_test
from bronze.crm_prd_info
where prd_key  in ('AC-HE-HL-U509-R','AC-HE-HL-U509')   


-------------------------DATA TRANSFORATION 
select 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') NOT IN (
select distinct id from bronze.erp_px_cat_g1v2)

select 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7,len(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where SUBSTRING(prd_key, 7,len(prd_key)) IN(
select sls_prd_key from bronze.crm_sales_details )

---checked for unwated spaces and nulls and 
select 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7,len(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line)) 
	 WHEN 'M 'THEN 'Mountain'
	 WHEN 'R'THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T'THEN 'Touring'
	 ELSE 'n/a'
END prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
/*where SUBSTRING(prd_key, 7,len(prd_key)) IN(
select sls_prd_key from bronze.crm_sales_details )*/

----check for invalid date orders
select 
prd_id,
REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id, --extract category ID 
SUBSTRING(prd_key, 7,len(prd_key)) AS prd_key, --extract product key 
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line)) 
	 WHEN 'M 'THEN 'Mountain'
	 WHEN 'R'THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T'THEN 'Touring'
	 ELSE 'n/a'
END prd_line, --Map product line codes ro descriptive values 
CAST(prd_start_dt AS DATE),
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS DATE) AS prd_end_dt -- Calculate end date as one day before the next date
from bronze.crm_prd_info

-----INSERT INTO  silver.crm_prd_info
INSERT INTO silver.crm_prd_info(
     prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7,len(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line)) 
	 WHEN 'M 'THEN 'Mountain'
	 WHEN 'R'THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T'THEN 'Touring'
	 ELSE 'n/a'
END prd_line,
CAST(prd_start_dt AS DATE),
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS DATE) AS prd_end_dt
from bronze.crm_prd_info



