
select
cid,
CASE WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END cid,
bdate,
gen
from bronze.erp_cust_az12
where CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END  NOT IN (select distinct  cst_key from silver.crm_cust_info)

--where cid NOT IN (select distinct  cst_key from silver.crm_cust_info)

select * from silver.crm_cust_info

select * from bronze.erp_cust_az12
where cid LIKE  '%AW00011002%'

--NASAW00011002

---date chcek for very old customers 
-- CHECK for birthdays in the future 
select bdate 
from bronze.erp_cust_az12
where bdate < '1924-01-01' OR   bdate > GETDATE()

select
CASE WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END cid,
bdate,
CASE WHEN bdate > GETDATE() THEN  NULL
	 ELSE bdate
END bdate ,
gen
from bronze.erp_cust_az12


---check gender
select distinct 
gen 
from bronze.erp_cust_az12

select
CASE WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END cid,
bdate,
CASE WHEN bdate > GETDATE() THEN  NULL
	 ELSE bdate
END bdate ,
gen, 
CASE WHEN UPPER(TRIM(gen)) IN  (' Male', 'M') THEN  'Male'
	 WHEN UPPER(TRIM(gen)) IN  ('Female', 'F')  THEN  'Female'
	 ELSE 'n/a'
END gen
from bronze.erp_cust_az12

select
CASE WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END cid,
bdate,
gen
from bronze.erp_cust_az12
-------
select
CASE WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN  NULL
	 ELSE bdate
END bdate,
gen
from bronze.erp_cust_az12

-----
SELECT 
CASE WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN  NULL
	 ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(gen)) IN  ('Male', 'M') THEN  'Male'
	 WHEN UPPER(TRIM(gen)) IN  ('Female', 'F')  THEN  'Female'
	 ELSE 'n/a'
END gen
FROM  bronze.erp_cust_az12


-----INSERT INTO silver.erp_cust_az12

INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT 
CASE WHEN cid LIKE 'NAS%'  THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN  NULL
	 ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(gen)) IN  ('Male', 'M') THEN  'Male'
	 WHEN UPPER(TRIM(gen)) IN  ('Female', 'F')  THEN  'Female'
	 ELSE 'n/a'
END gen
FROM  bronze.erp_cust_az12

