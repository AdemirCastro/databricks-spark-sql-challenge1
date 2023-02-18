# Databricks notebook source
# MAGIC %md
# MAGIC #ADEMIR CASTRO - Data Engineer & DevOps
# MAGIC Linkedin: https://www.linkedin.com/in/ademircastro/

# COMMAND ----------

from zipfile import ZipFile
from pyspark.sql.functions import col, explode, sequence, to_date
from pyspark.sql.utils import AnalysisException
import csv

# Files uploaded
# dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_cartentries.zip
# dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_addresses.zip
# dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_paymentinfos.zip
# dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_carts.zip
# dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_cmssitelp.csv
# dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_paymentmodes.csv
# dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_regions.csv
# dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_users.csv

# COMMAND ----------

def read_zipped_parquet(filename: str) -> spark.read.parquet:
    dbutils.fs.cp(f"dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/{filename}.zip", f"file:/databricks/driver/{filename}.zip")
    ZipFile(f'{filename}.zip').extractall()
    dbutils.fs.rm(f"file:/databricks/driver/{filename}.zip")
    dbutils.fs.mv(f"file:/databricks/driver/{filename}.parquet",f"dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/{filename}.parquet")
    return spark.read.parquet(f"dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/{filename}.parquet")

def export_as_txt(df, filename: str, delimiter='|') -> None:    
    df.coalesce(1).write.mode('overwrite').option("header",True).option("delimiter",delimiter).csv(f"/FileStore/shared_uploads/oademircastro@gmail.com/{filename}.csv")
    data_location = f"/FileStore/shared_uploads/oademircastro@gmail.com/{filename}.csv/"
    files = dbutils.fs.ls(data_location)
    csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
    dbutils.fs.cp(csv_file,f'file:/databricks/driver/{filename}.csv')
    f = open(f'{filename}.csv', encoding='UTF8')
    csv_file = csv.reader(f,delimiter=delimiter)
    with open(f'{filename}.txt', "w") as my_output_file:
        for row in csv_file:
            my_output_file.write(delimiter.join(row)+'\n')
        my_output_file.close()
    dbutils.fs.mv(f'file:/databricks/driver/{filename}.txt',f'/FileStore/tables/{filename}.txt')
    dbutils.fs.rm(f'file:/databricks/driver/{filename}.csv')
    displayHTML(f"""<a href="https://community.cloud.databricks.com/files/tables/{filename}.txt?o=1582075949323120" download>CLICK HERE TO DOWNLOAD TXT FILE</a>""")
    return None

# COMMAND ----------

try:
    spark.sql('CREATE DATABASE delta')
except AnalysisException:
    pass

df_cmssitelp    = spark.read.format("csv").option("header", "true").option("sep","|").load("dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_cmssitelp.csv")
df_paymentmodes = spark.read.format("csv").option("header", "true").option("sep","|").load("dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_paymentmodes.csv")
df_regions      = spark.read.format("csv").option("header", "true").option("sep","|").load("dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_regions.csv")
df_users        = spark.read.format("csv").option("header", "true").option("sep","|").load("dbfs:/FileStore/shared_uploads/oademircastro@gmail.com/tb_users.csv")
df_cartentries  = read_zipped_parquet('tb_cartentries')
df_addresses    = read_zipped_parquet('tb_addresses')
df_paymentinfos = read_zipped_parquet('tb_paymentinfos')
df_carts        = read_zipped_parquet('tb_carts')

df_cmssitelp.write.format("delta").mode("overwrite").saveAsTable("delta.cmssitelp")
df_paymentmodes.write.format("delta").mode("overwrite").saveAsTable("delta.paymentmodes")
df_regions.write.format("delta").mode("overwrite").saveAsTable("delta.regions")
df_users.write.format("delta").mode("overwrite").saveAsTable("delta.users")
df_cartentries.write.format("delta").mode("overwrite").saveAsTable("delta.cartentries")
df_addresses.write.format("delta").mode("overwrite").saveAsTable("delta.addresses")
df_paymentinfos.write.format("delta").mode("overwrite").saveAsTable("delta.paymentinfos")
df_carts.write.format("delta").mode("overwrite").saveAsTable("delta.carts")

begin_date = '2000-01-01'
end_date = '2100-01-01'
spark.sql(f"select explode(sequence(to_date('{begin_date}'), to_date('{end_date}'), interval 1 day)) as calendar_date").write.format("delta").mode("overwrite").saveAsTable("delta.calendar")

carts_last_datetime = spark.sql("""
SELECT MAX(carts.createdTS) as last_datetime
FROM delta.carts
""").first().last_datetime
spark.conf.set('carts.last_datetime',carts_last_datetime)

del df_cmssitelp
del df_paymentmodes
del df_regions
del df_users
del df_cartentries
del df_addresses
del df_paymentinfos
del df_carts

# COMMAND ----------

# MAGIC %md
# MAGIC ### PRELIMINARY OBSERVATIONS
# MAGIC - carts.p_paymentstatus has only NULL values.
# MAGIC - carts.p_deliverystatus has only NULL values.
# MAGIC - region.p_ibgecode has so many nan values. This can make it difficult to identify the location in some applications, such as a shipping calculator. Whe can get a complete table of cities, districts, official names and codes in Brazil, in the IBGE table: https://www.ibge.gov.br/explica/codigos-dos-municipios.php
# MAGIC - cartentries and carts tables creation timestamp columns have different values for same order. Because this, we are using carts table timestamp on both cartentries and carts, during the analysis.
# MAGIC - there is no way to uniquely identifiy a item on cartentries. Because of this, we cannot get the exact lost value on abandonments during a period of time. The same item can be abandoned several times in a month, as example.
# MAGIC - because the divergence on carts and cartentries quantities, this analysis can be extremely biased or broken. It is recommended to redo the analysis with the complete dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEFINING CRITERIA TO A VALID CART AND ABANDONED CART
# MAGIC #####To start our analysis, we must define the criteria for valid and abanoned carts. For this, we start observing a problem:

# COMMAND ----------

display(spark.sql("""
SELECT COUNT(DISTINCT(carts.PK)) NUMBER_OF_CARTS
FROM delta.carts
"""))

display(spark.sql("""
SELECT COUNT(DISTINCT(cartentries.p_order)) NUMBER_OF_CART_ENTRIES
FROM delta.cartentries
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC #### As you can see, we have much more carts than cart entries. For this, we have two possible scenarios: either we have cartentries table incomplete, or we have empty carts. If we assume that empty carts are those with no total price and no subtotal price, then we discover that the second scenario is valid:

# COMMAND ----------

display(spark.sql("""
SELECT COUNT(DISTINCT(PK)) AS CARTS_WITH_NO_PRICE
FROM delta.carts
WHERE (p_subtotalwithoutdiscounts = 0 OR p_subtotalwithoutdiscounts IS NULL) AND (p_totalprice = 0 OR p_totalprice IS NULL )
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Eliminating carts with no price, we have the more realistic scenario:

# COMMAND ----------

display(spark.sql("""
SELECT COUNT(DISTINCT(carts.PK))AS NUMBER_OF_CARTS
FROM delta.carts
WHERE p_subtotalwithoutdiscounts > 0 OR p_totalprice > 0
"""))

display(spark.sql("""
SELECT COUNT(DISTINCT(cartentries.p_order)) AS NUMBER_OF_CART_ENTRIES
FROM delta.cartentries
RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
WHERE carts.p_subtotalwithoutdiscounts > 0 OR carts.p_totalprice > 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC #### We still have more carts than carentries with identification with a cart, but now we have a much better error margin than the original scenario. So lets clean the carts table and start our analysis.

# COMMAND ----------

spark.sql("""
SELECT *
FROM delta.carts
WHERE carts.p_subtotalwithoutdiscounts > 0 OR carts.p_totalprice > 0
""").write.format("delta").mode("overwrite").saveAsTable("delta.carts")

# COMMAND ----------

# MAGIC %md
# MAGIC #Lets define a abandoned cart as a cart with no payment info. Then we have:

# COMMAND ----------

# MAGIC %md
# MAGIC ##TOP 100 PRODUCTS WITH MOST ABANDONED CARTS

# COMMAND ----------

display(spark.sql("""
SELECT cartentries.p_product, COUNT(DISTINCT carts.PK) as total_abandoned_carts
FROM delta.cartentries
RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
WHERE carts.p_paymentinfo IS NULL
GROUP BY cartentries.p_product
ORDER BY total_abandoned_carts DESC
LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TOP 100 PRODUCTS PAIRS WITH MOST ABANDONED CARTS

# COMMAND ----------

display(spark.sql("""
SELECT product1, product2, total_abandoned_carts
FROM (
    SELECT cartentries1.p_product as product1, cartentries2.p_product as product2, COUNT(DISTINCT carts.PK) as total_abandoned_carts, 
           row_number() OVER (PARTITION BY CASE
                                         WHEN cartentries1.p_product < cartentries2.p_product THEN CONCAT(CAST(cartentries1.p_product AS varchar(50)), CAST(cartentries2.p_product AS varchar(50)))
                                         ELSE CONCAT(CAST(cartentries2.p_product AS varchar(50)), CAST(cartentries1.p_product as varchar(50)))
                                         END ORDER BY cartentries1.p_product,cartentries2.p_product) AS rank
    FROM delta.cartentries as cartentries1, delta.cartentries as cartentries2 
    RIGHT JOIN delta.carts ON cartentries1.p_order = carts.PK
    WHERE carts.p_paymentinfo IS NULL AND cartentries1.p_order = cartentries2.p_order AND cartentries1.p_product <> cartentries2.p_product
    GROUP BY cartentries1.p_product, cartentries2.p_product 
)
WHERE rank = 1
ORDER BY total_abandoned_carts DESC
LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##TOP 100 PRODUCTS WITH THE HIGHEST ABANDONMENT INCREASE BETWEEN THE MONTH BEFORE LAST AND THE LAST MONTH, BASED ON LAST CART DATETIME

# COMMAND ----------

display(spark.sql("""
SELECT abandonment_last_month.p_product, 
       COALESCE(abandonment_month_before_last.total_abandoned_carts,0) as abandoned_carts_month_before_last, 
       COALESCE(abandonment_last_month.total_abandoned_carts,0) as abandoned_carts_last_month, 
       COALESCE(abandonment_last_month.total_abandoned_carts,0) - COALESCE(abandonment_month_before_last.total_abandoned_carts,0) as increase
FROM (
    SELECT cartentries.p_product, COUNT(DISTINCT carts.PK) as total_abandoned_carts
    FROM delta.cartentries
    RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE carts.p_paymentinfo IS NULL AND YEAR(carts.createdTS) = YEAR(DATEADD(MONTH,-1,'${carts.last_datetime}')) AND MONTH(carts.createdTS) = MONTH(DATEADD(MONTH,-1,'${carts.last_datetime}'))
    GROUP BY cartentries.p_product
) AS abandonment_last_month
LEFT JOIN (
    SELECT cartentries.p_product, COUNT(DISTINCT carts.PK) as total_abandoned_carts
    FROM delta.cartentries
    RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE carts.p_paymentinfo IS NULL AND YEAR(carts.createdTS) = YEAR(DATEADD(MONTH,-2,'${carts.last_datetime}')) AND MONTH(carts.createdTS) = MONTH(DATEADD(MONTH,-2,'${carts.last_datetime}'))
    GROUP BY cartentries.p_product
) AS abandonment_month_before_last ON abandonment_last_month.p_product = abandonment_month_before_last.p_product
GROUP BY abandonment_last_month.p_product, abandoned_carts_month_before_last, abandoned_carts_last_month
HAVING increase > 0
ORDER BY increase DESC
LIMIT 100;
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##TOP 100 PRODUCTS WITH THE HIGHEST ABANDONMENT INCREASE BETWEEN THE LAST YEAR AND THE CURRENT YEAR, BASED ON LAST CART DATETIME

# COMMAND ----------

display(spark.sql("""
SELECT abandonment_last_year.p_product, 
       COALESCE(abandonment_last_year.total_abandoned_carts,0) as abandoned_carts_last_year, 
       COALESCE(abandonment_current_year.total_abandoned_carts,0) as abandoned_carts_current_year, 
       COALESCE(abandonment_current_year.total_abandoned_carts,0) - COALESCE(abandonment_last_year.total_abandoned_carts,0) as increase
FROM (
    SELECT cartentries.p_product, COUNT(DISTINCT carts.PK) as total_abandoned_carts
    FROM delta.cartentries
    LEFT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE carts.p_paymentinfo IS NULL AND YEAR(carts.createdTS) = YEAR('${carts.last_datetime}')
    GROUP BY cartentries.p_product
    ) abandonment_current_year
RIGHT JOIN (
    SELECT cartentries.p_product, COUNT(DISTINCT carts.PK) as total_abandoned_carts
    FROM delta.cartentries
    LEFT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE carts.p_paymentinfo IS NULL AND YEAR(carts.createdTS) = YEAR(DATEADD(YEAR,-1,'${carts.last_datetime}'))
    GROUP BY cartentries.p_product
    ) as abandonment_last_year ON abandonment_current_year.p_product = abandonment_last_year.p_product
GROUP BY abandonment_last_year.p_product, abandonment_last_year.total_abandoned_carts, abandonment_current_year.total_abandoned_carts
HAVING increase > 0
ORDER BY increase DESC
LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##TOP STATES WITH MOST ABANDONED CARTS

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Under the 937047 abandoned carts (with NULL payment info), we have 937046 carts with NULL payment address, 901200 carts with NULL delivery address, and 170212 carts with nan zipcodecalculatedelivery. The table regions is the only with state information, and to reach it is necessary a relationship with the table address. Therefore we cannot get a consistent rank of states with most abandoned carts. We can validate this below:

# COMMAND ----------

# MAGIC %md
# MAGIC ######TOP STATES WITH MOST ABANDONED CARTS (BY PAYMENT ADDRESS)

# COMMAND ----------

display(spark.sql("""
SELECT regions.p_isocodeshort, SUM(abandonment_address.total_abandoned_carts) as total_abandoned_carts
FROM (
    SELECT addresses.p_region, COUNT(DISTINCT carts.PK) as total_abandoned_carts
    FROM delta.carts
    LEFT JOIN delta.addresses ON carts.p_paymentaddress = addresses.PK
    WHERE carts.p_paymentinfo IS NULL
    GROUP BY addresses.p_region
) AS abandonment_address
FULL JOIN delta.regions ON abandonment_address.p_region = regions.PK
GROUP BY regions.p_isocodeshort
ORDER BY total_abandoned_carts DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ######TOP STATES WITH MOST ABANDONED CARTS (BY DELIVERY ADDRESS)

# COMMAND ----------

display(spark.sql("""
SELECT regions.p_isocodeshort, SUM(abandonment_address.total_abandoned_carts) as total_abandoned_carts
FROM (
    SELECT addresses.p_region, COUNT(DISTINCT carts.PK) as total_abandoned_carts
    FROM delta.carts
    LEFT JOIN delta.addresses ON carts.p_deliveryaddress = addresses.PK
    WHERE carts.p_paymentinfo IS NULL
    GROUP BY addresses.p_region
) AS abandonment_address
FULL JOIN delta.regions ON abandonment_address.p_region = regions.PK
GROUP BY regions.p_isocodeshort
ORDER BY total_abandoned_carts DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ######TOP STATES WITH MOST ABANDONED CARTS (BY ZIP CODE)

# COMMAND ----------

display(spark.sql("""
SELECT regions.p_isocodeshort, SUM(abandonment_address.total_abandoned_carts) as total_abandoned_carts
FROM (
    SELECT addresses.p_region, COUNT(DISTINCT carts.PK) as total_abandoned_carts
    FROM delta.carts
    LEFT JOIN delta.addresses ON CONCAT(LEFT(carts.p_zipcodecalculatedelivery,5),'-',RIGHT(carts.p_zipcodecalculatedelivery,3)) = addresses.p_postalcode
    WHERE carts.p_paymentinfo IS NULL
    GROUP BY addresses.p_region
) AS abandonment_address
FULL JOIN delta.regions ON abandonment_address.p_region = regions.PK
GROUP BY regions.p_isocodeshort
ORDER BY total_abandoned_carts DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##NEW PRODUCTS, CARTS ON LAUNCH MONTH AND ABANDONED CARTS ON LAUNCH MONTH, BASED ON LAST CART DATETIME

# COMMAND ----------

# MAGIC %md
# MAGIC #####PRODUCTS LAUNCHED IN AT LEAST LAST MONTH, CARTS ON LAUNCH MONTH AND ABANDONED CARTS ON LAUNCH MONTH, BASED ON LAST CART DATETIME

# COMMAND ----------

display(spark.sql("""

SELECT launch_carts.p_product, launch_carts.launch_year, launch_carts.launch_month,
       COALESCE(launch_carts.launch_month_carts,0) AS launch_month_carts, COALESCE(launch_abandoned_carts.launch_month_abandoned_carts,0) AS launch_month_abandoned_carts
FROM (
    SELECT launch_products.p_product, launch_products.launch_year, launch_products.launch_month, COUNT(DISTINCT(carts.PK)) as launch_month_carts
    FROM (
        SELECT cartentries.p_product, YEAR(MIN(carts.createdTS)) AS launch_year, MONTH(MIN(carts.createdTS)) AS launch_month
        FROM delta.cartentries
        RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
        GROUP BY cartentries.p_product
        HAVING launch_year = YEAR(DATEADD(MONTH,-1,'${carts.last_datetime}')) AND launch_month = MONTH(DATEADD(MONTH,-1,'${carts.last_datetime}'))
    ) AS launch_products
    RIGHT JOIN delta.cartentries ON launch_products.p_product = cartentries.p_product
    RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE YEAR(carts.createdTS) = launch_products.launch_year AND MONTH(carts.createdTS) = launch_products.launch_month
    GROUP BY launch_products.p_product, launch_products.launch_year, launch_products.launch_month
) AS launch_carts
LEFT JOIN (
    SELECT launch_products.p_product, launch_products.launch_year, launch_products.launch_month, COUNT(DISTINCT(carts.PK)) as launch_month_abandoned_carts
    FROM (
        SELECT cartentries.p_product, YEAR(MIN(carts.createdTS)) AS launch_year, MONTH(MIN(carts.createdTS)) AS launch_month
        FROM delta.cartentries
        RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
        GROUP BY cartentries.p_product
        HAVING launch_year = YEAR(DATEADD(MONTH,-1,'${carts.last_datetime}')) AND launch_month = MONTH(DATEADD(MONTH,-1,'${carts.last_datetime}'))
    ) AS launch_products
    RIGHT JOIN delta.cartentries ON launch_products.p_product = cartentries.p_product
    RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE launch_products.launch_year = YEAR(carts.createdTS) AND launch_products.launch_month = MONTH(carts.createdTS) AND carts.p_paymentinfo IS NULL
    GROUP BY launch_products.p_product, launch_products.launch_year, launch_products.launch_month
) AS launch_abandoned_carts ON launch_carts.p_product = launch_abandoned_carts.p_product
ORDER BY launch_month_carts DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC #####PRODUCTS LAUNCHED IN AT LEAST LAST YEAR, CARTS ON LAUNCH MONTH AND ABANDONED CARTS ON LAUNCH MONTH, BASED ON LAST CART DATETIME

# COMMAND ----------

display(spark.sql("""

SELECT launch_carts.p_product, launch_carts.launch_year, launch_carts.launch_month,
       COALESCE(launch_carts.launch_month_carts,0) AS launch_month_carts, COALESCE(launch_abandoned_carts.launch_month_abandoned_carts,0) AS launch_month_abandoned_carts
FROM (
    SELECT launch_products.p_product, launch_products.launch_year, launch_products.launch_month, COUNT(DISTINCT(carts.PK)) as launch_month_carts
    FROM (
        SELECT cartentries.p_product, YEAR(MIN(carts.createdTS)) AS launch_year, MONTH(MIN(carts.createdTS)) AS launch_month
        FROM delta.cartentries
        RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
        GROUP BY cartentries.p_product
        HAVING launch_year = YEAR(DATEADD(YEAR,-1,'${carts.last_datetime}'))
    ) AS launch_products
    RIGHT JOIN delta.cartentries ON launch_products.p_product = cartentries.p_product
    RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE YEAR(carts.createdTS) = launch_products.launch_year AND MONTH(carts.createdTS) = launch_products.launch_month
    GROUP BY launch_products.p_product, launch_products.launch_year, launch_products.launch_month
) AS launch_carts
LEFT JOIN (
    SELECT launch_products.p_product, launch_products.launch_year, launch_products.launch_month, COUNT(DISTINCT(carts.PK)) as launch_month_abandoned_carts
    FROM (
        SELECT cartentries.p_product, YEAR(MIN(carts.createdTS)) AS launch_year, MONTH(MIN(carts.createdTS)) AS launch_month
        FROM delta.cartentries
        RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
        GROUP BY cartentries.p_product
        HAVING launch_year = YEAR(DATEADD(YEAR,-1,'${carts.last_datetime}'))
    ) AS launch_products
    RIGHT JOIN delta.cartentries ON launch_products.p_product = cartentries.p_product
    RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE launch_products.launch_year = YEAR(carts.createdTS) AND launch_products.launch_month = MONTH(carts.createdTS) AND carts.p_paymentinfo IS NULL
    GROUP BY launch_products.p_product, launch_products.launch_year, launch_products.launch_month
) AS launch_abandoned_carts ON launch_carts.p_product = launch_abandoned_carts.p_product
ORDER BY launch_month_carts DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TOTAL ABANDONED CARTS COUNT, TOTAL ABANDONED ITENS COUNT, AND TOTAL VALUE, BY YEAR AND MONTH, IN THE LAST 12 MONTHS BASED ON LAST CART DATETIME
# MAGIC #### THE RELATORIES ARE IN THE DASHBOARDS OF THIS NOTEBOOK

# COMMAND ----------

df = spark.sql("""
SELECT CONCAT(CAST(YEAR(calendar.calendar_date) AS varchar(4)),'-',RIGHT(CONCAT('0',CAST(MONTH(calendar.calendar_date) AS varchar(2))),2)) AS year_month,
       COALESCE(SUM(carts_abandonments.total_abandoned_carts),0) AS total_abandoned_carts, COALESCE(SUM(carts_abandonments.total_value_carts),0) AS total_value_carts,
       COALESCE(SUM(itens_abandonments.total_abandoned_itens),0) AS total_abandoned_itens, COALESCE(SUM(itens_abandonments.total_value_itens),0) AS total_value_itens
FROM delta.calendar
LEFT JOIN (
    SELECT DATE(carts.createdTS) AS DATE, COUNT(DISTINCT carts.PK) AS total_abandoned_carts, SUM(carts.p_totalprice) AS total_value_carts
    FROM delta.carts
    WHERE carts.p_paymentinfo IS NULL
    GROUP BY DATE
) AS carts_abandonments ON calendar.calendar_date = carts_abandonments.DATE
LEFT JOIN (
    SELECT DATE(carts.createdTS) AS DATE, SUM(cartentries.p_quantity) AS total_abandoned_itens, SUM(cartentries.p_totalprice) AS total_value_itens
    FROM delta.cartentries
    RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE carts.p_paymentinfo IS NULL
    GROUP BY DATE
) AS itens_abandonments ON carts_abandonments.DATE = itens_abandonments.DATE
WHERE calendar.calendar_date >= DATEADD(DAY,1-DAY(DATEADD(MONTH,-11,'${carts.last_datetime}')),DATEADD(MONTH,-11,'${carts.last_datetime}')) AND calendar.calendar_date <= '${carts.last_datetime}'
GROUP BY year_month
ORDER BY year_month DESC, total_value_carts DESC, total_value_itens DESC
""")

df.write.format("delta").mode("overwrite").saveAsTable("delta.abandonment_analytics_month")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.abandonment_analytics_month

# COMMAND ----------

# MAGIC %md
# MAGIC ## TOTAL ABANDONED CARTS COUNT, TOTAL ABANDONED ITENS COUNT, AND TOTAL VALUE, BY DATE, IN THE LAST 3 MONTHS BASED ON LAST CART DATETIME
# MAGIC #### THE RELATORIES ARE IN THE DASHBOARDS OF THIS NOTEBOOK

# COMMAND ----------

df = spark.sql("""
SELECT calendar.calendar_date AS date,
       COALESCE(SUM(carts_abandonments.total_abandoned_carts),0) AS total_abandoned_carts, COALESCE(SUM(carts_abandonments.total_value_carts),0) AS total_value_carts,
       COALESCE(SUM(itens_abandonments.total_abandoned_itens),0) AS total_abandoned_itens, COALESCE(SUM(itens_abandonments.total_value_itens),0) AS total_value_itens
FROM delta.calendar
LEFT JOIN (
    SELECT DATE(carts.createdTS) AS DATE, COUNT(DISTINCT carts.PK) AS total_abandoned_carts, SUM(carts.p_totalprice) AS total_value_carts
    FROM delta.carts
    WHERE carts.p_paymentinfo IS NULL
    GROUP BY DATE
) AS carts_abandonments ON calendar.calendar_date = carts_abandonments.DATE
LEFT JOIN (
    SELECT DATE(carts.createdTS) AS DATE, SUM(cartentries.p_quantity) AS total_abandoned_itens, SUM(cartentries.p_totalprice) AS total_value_itens
    FROM delta.cartentries
    RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
    WHERE carts.p_paymentinfo IS NULL
    GROUP BY DATE
) AS itens_abandonments ON carts_abandonments.DATE = itens_abandonments.DATE
WHERE calendar.calendar_date >= DATEADD(DAY,1-DAY(DATEADD(MONTH,-2,'${carts.last_datetime}')),DATEADD(MONTH,-2,'${carts.last_datetime}')) AND calendar.calendar_date <= '${carts.last_datetime}'
GROUP BY calendar.calendar_date
ORDER BY calendar.calendar_date DESC, total_value_carts DESC, total_value_itens DESC
""")

df.write.format("delta").mode("overwrite").saveAsTable("delta.abandonment_analytics_day")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.abandonment_analytics_day

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXPORT TXT TABLE WITH LAYOUT carts.PK|carts.createdTS|carts.p_totalprice|user.p_uid|paymentmodes.p_code|paymentinfos.p_installments|cmssitelp.p_name|
# MAGIC ##addresses.p_postalcode|sum(cartentries.p_quantity)|count(cartentries.PK)

# COMMAND ----------

df = spark.sql("""
SELECT carts.PK as cart_PK, carts.createdTS as cart_createdTS, carts.p_totalprice as cart_totalprice, users.p_uid as user_uid, paymentmodes.p_code as paymentmode_code, paymentinfos.p_installments as paymentinfo_installmentes, cmssitelp.p_name as cmssitelp_name, addresses.p_postalcode as address_postalcode, COALESCE(SUM(cartentries.p_quantity),0) as sum_quantity, COUNT(cartentries.PK) as count_cartentries
FROM delta.cartentries
RIGHT JOIN delta.carts ON cartentries.p_order = carts.PK
RIGHT JOIN delta.users ON carts.p_user = users.PK
LEFT JOIN delta.paymentmodes ON carts.p_paymentmode = paymentmodes.PK
LEFT JOIN delta.paymentinfos ON carts.p_paymentinfo = paymentinfos.PK
RIGHT JOIN delta.cmssitelp ON carts.p_site = cmssitelp.ITEMPK
LEFT JOIN delta.addresses ON carts.p_paymentaddress = addresses.PK
GROUP BY carts.PK, carts.createdTS, carts.p_totalprice, users.p_uid, paymentmodes.p_code, paymentinfos.p_installments, cmssitelp.p_name, addresses.p_postalcode
ORDER BY cart_totalprice DESC
LIMIT 50
""")

display(df)
export_as_txt(df, filename='carts', delimiter='|')

# COMMAND ----------

# MAGIC %md
# MAGIC ### If the notebook had been detached from cluster, you can download the txt table in the link: 
# MAGIC https://drive.google.com/file/d/1GYrfi6smdmVwaiHu3YpzNIXX7mS_2f9V/view?usp=sharing
