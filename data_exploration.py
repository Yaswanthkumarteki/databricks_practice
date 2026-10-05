# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("show schemas in samples").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in samples.tpch;

# COMMAND ----------

spark.sql("describe extended samples.tpch.customer").display()

# COMMAND ----------

df_customer = spark.read.table("samples.tpch.customer")
display(df_customer)

# COMMAND ----------

from pyspark.sql.functions import *
from functools import reduce
df_customer_nulls = df_customer.where(reduce(lambda a, b: a | b, [col(c).isNull() for c in df_customer.columns]))
df_customer_nulls.display()

# COMMAND ----------

display(df_customer.select("c_custkey").distinct().count())

# COMMAND ----------

df_customer.select(max(col("c_custkey")), min(col("c_custkey"))).display()

# COMMAND ----------

df_customer.select("*").where(col("c_custkey") == 750000).display()

# COMMAND ----------

df_customer.select(col("c_mktsegment")).distinct().display()

# COMMAND ----------

df_grouped = df_customer.select(max(col("c_acctbal")).alias("max_balance"), min(col("c_acctbal")).alias("min_balance"), avg(col("c_acctbal")).alias("avg_balance")).display()

# COMMAND ----------

display(df_grouped)

# COMMAND ----------

display(spark.sql("select max(c_nationkey), min(c_nationkey) from samples.tpch.customer"))

# COMMAND ----------

df_customer.select("*").where(col('c_nationkey').isNull()).display()

# COMMAND ----------

df_nation = spark.read.table("samples.tpch.nation")
display(df_nation)

# COMMAND ----------

df_nation.count()

# COMMAND ----------

spark.sql("describe extended samples.tpch.nation").display()

# COMMAND ----------

df_nation_nulls = df_nation.where(reduce(lambda x,y:x|y, [col(c).isNull() for c in df_nation.columns]))
df_nation_nulls.display()

# COMMAND ----------

df_nation.select("n_nationkey").distinct().display()

# COMMAND ----------

spark.sql("select n_regionkey,count(n_name) from samples.tpch.nation group by n_regionkey").display()

# COMMAND ----------

df_nation_grouped = df_nation.groupBy(col("n_regionkey")).agg(count("n_name").alias("count_of_nations")).display()

# COMMAND ----------

spark.sql("show tables in samples.tpch").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended samples.tpch.region

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.tpch.region

# COMMAND ----------

df_region = spark.read.table("samples.tpch.region")
display(df_region.filter(reduce(lambda x,y:x|y, [col(c).isNull() for c in df_region.columns])))

# COMMAND ----------

df_region.select(max("r_regionkey")).display()

# COMMAND ----------

df_nation_region_joined = df_nation.join(df_region,how ="left_semi", on = df_nation.n_regionkey == df_region.r_regionkey)
display(df_nation_region_joined)

# COMMAND ----------

df_orders = spark.read.table("samples.tpch.orders")
display(df_orders.count())

# COMMAND ----------

spark.sql("describe extended samples.tpch.orders").display()

# COMMAND ----------

df_customer_orders_joined = df_customer.join(df_orders, how = "left", on=df_customer.c_custkey == df_orders.o_custkey)
display(df_customer_orders_joined.display())

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct t.c) from (select c.c_custkey c from samples.tpch.customer c left join samples.tpch.orders o on c.c_custkey = o.o_custkey
# MAGIC where o.o_orderkey is null) t

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from (select o.o_orderkey from samples.tpch.orders o left join samples.tpch.customer c on c.c_custkey = o.o_custkey
# MAGIC where c.c_custkey is null) t

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(o_orderstatus) from samples.tpch.orders;

# COMMAND ----------

df_orders_grouped = df_orders.groupBy(col("o_orderstatus")).agg(count('*').alias("orders_count_per_ststus")).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(o_totalprice), min(o_totalprice), avg(o_totalprice) from samples.tpch.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ####Find the number of orders placed by each customer:
# MAGIC ###   
# MAGIC ####  What is the minimum number of orders placed by any customer, and what is the maximum number of orders placed by any customer?

# COMMAND ----------

df_customer_orders_joined.groupBy(col("c_custkey")).agg(count('*').alias("orders_per_customer")).select(max("orders_per_customer").alias("max_orders"), min("orders_per_customer").alias("min_orders")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Find the customer(s) who placed the maximum number of orders**

# COMMAND ----------

df_customer_orders_joined.groupBy(col("c_custkey")).agg(count('*').alias("orders_per_customer")).select("c_custkey", "orders_per_customer").orderBy("orders_per_customer", ascending=False).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select * from (select t.o_custkey, t.cnt, dense_rank() over(order by t.cnt desc) as rnk from (select o_custkey, count(o_orderkey) cnt from samples.tpch.orders left join samples.tpch.customer
# MAGIC on samples.tpch.orders.o_custkey = samples.tpch.customer.c_custkey
# MAGIC group by samples.tpch.orders.o_custkey
# MAGIC order by cnt desc) t)t1 where rnk = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **customer(s) who placed the maximum number of orders.**

# COMMAND ----------

df_customer.select("c_custkey","c_name", "c_nationkey","c_mktsegment", "c_acctbal"). where(col("c_custkey").isin(df_customer_orders_joined.groupBy(col("c_custkey")).agg(count('*').alias("orders_per_customer")).select("c_custkey").orderBy("orders_per_customer", ascending=False).limit(2))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### For these two customers, find their nation names.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.tpch.region limit 1;
# MAGIC select * from samples.tpch.nation limit 1;
# MAGIC select * from samples.tpch.customer limit 1;
# MAGIC select * from samples.tpch.orders limit 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **For those two customers, find their region names.**

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.c_custkey, c.c_name,n.n_name,r_name
# MAGIC from samples.tpch.customer c
# MAGIC left join samples.tpch.nation n
# MAGIC on c.c_nationkey = n.n_nationkey
# MAGIC left join samples.tpch.region r
# MAGIC on n.n_regionkey = r.r_regionkey
# MAGIC where c.c_custkey in 
# MAGIC (select o_custkey from (select t.o_custkey, t.cnt, dense_rank() over(order by t.cnt desc) as rnk from (select o_custkey, count(o_orderkey) cnt from samples.tpch.orders left join samples.tpch.customer
# MAGIC on samples.tpch.orders.o_custkey = samples.tpch.customer.c_custkey
# MAGIC group by samples.tpch.orders.o_custkey
# MAGIC order by cnt desc) t)t1 where rnk = 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Exploring lineitem table**

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended samples.tpch.lineitem

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.tpch.lineitem limit 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select l_shipmode, sum(l_extendedprice * (1-l_discount)) as total_revenue
# MAGIC from samples.tpch.lineitem
# MAGIC group by l_shipmode
# MAGIC ;

# COMMAND ----------

df_shipmode = spark.read.table("samples.tpch.lineitem").withColumn("revenue", col("l_extendedprice") * (1 - col("l_discount"))).groupBy("l_shipmode").agg(sum("revenue").alias("revenue")).orderBy("revenue", ascending=False)

df_shipmode.display()

                                                                                        

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Revenue by order Now go one level deeper.For each order, calculate its total revenue from lineitem**

# COMMAND ----------

df_shipmode_with_revenue = spark.read.table("samples.tpch.lineitem").withColumn("revenue", col("l_extendedprice") * (1 - col("l_discount")))

df_shipmode_joined_order = df_shipmode_with_revenue.join(df_orders, on = df_shipmode_with_revenue.l_orderkey == df_orders.o_orderkey, how = "inner")
# display(df_shipmode_joined_order)

df_shipmode_joined_order.select("o_orderkey", "o_custkey", "o_orderdate", "o_orderstatus", "revenue").orderBy("revenue", ascending = False).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Task 22 — Customer lifetime revenue ---- Calculate the total revenue generated by each customer across all their orders.**

# COMMAND ----------

df_shipmode_joined_order_customer = df_shipmode_joined_order.join(df_customer, on = df_shipmode_joined_order.o_custkey == df_customer.c_custkey, how = "inner")
df_shipmode_joined_order_customer.groupBy("c_custkey").agg(sum("revenue").alias("revenue")).orderBy(desc("revenue")).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Task 23 — Customer ranking within each market segment**

# COMMAND ----------

windowspec = Window.partitionBy("c_mktsegment").orderBy(desc("revenue"))
# df_shipmode_joined_order_customer.display()


df_shipmode_joined_order_customer.select("c_custkey","c_name","c_mktsegment","revenue",rank().over(windowspec).alias("rank")).filter(col('rank') <= 3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Task 24 — Customer spending behavior**

# COMMAND ----------

display(df_shipmode_joined_order_customer.select("*").limit(1))

# COMMAND ----------

df_shipmode_joined_order_customer.createTempView("shipmode_joined_order_customer")


# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC sum(case when total_revenue >= 5000000 then 1 else 0 end) as high_value,
# MAGIC sum(case when total_revenue >= 2500000 and total_revenue <= 5000000 then 1 else 0 end) medium_value,
# MAGIC sum(case when total_revenue < 2500000 then 1 else 0 end) as low_value
# MAGIC from( 
# MAGIC select 
# MAGIC count(l_orderkey) no_of_orders,
# MAGIC sum(revenue) total_revenue,
# MAGIC avg(revenue) average_revenue
# MAGIC from shipmode_joined_order_customer
# MAGIC group by c_custkey);

# COMMAND ----------

df_shipmode_joined_order_customer.groupBy('c_custkey').agg(sum('revenue').alias('total_revenue'),count('*').alias('total_orders'),avg('revenue').alias('avg_revenue')).withColumn('cat', when(col('total_orders') >= 5000000, 'High').when((col('total_orders') >= 2500000) & (col('total_orders') < 5000000), 'Medium').otherwise('Low')).orderBy('cat', Ascending = False).display()


# COMMAND ----------

display(df_shipmode_joined_order_customer)

# COMMAND ----------

df_shipmode_joined_order_customer.groupBy('c_custkey').agg(sum('revenue').alias('total_revenue'),count('*').alias('total_orders'),avg('revenue').alias('avg_revenue')).withColumn('cat', when(col('total_orders') >= 5000000, 'High').when((col('total_orders') >= 2500000) & (col('total_orders') < 5000000), 'Medium').otherwise('Low')).orderBy('cat').display()

# COMMAND ----------

df_shipmode_joined_order_customer.groupBy('c_custkey').agg(sum('revenue').alias('total_revenue'),count('*').alias('total_orders'),avg('revenue').alias('avg_revenue')).withColumn('cat', when(col('total_revenue') >= 5000000, 'High').when((col('total_revenue') >= 2500000) & (col('total_revenue') < 5000000), 'Medium').otherwise('Low')).orderBy('cat', Ascending = True).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select o_custkey, year(o_orderdate) yd from samples.tpch.orders group by o_custkey, yd order by o_custkey;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Task 25 - Customer retention**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(select o_custkey,min(yd) over(partition by o_custkey) mi,
# MAGIC max(yd) over( partition by o_custkey) ma, row_number() over (partition by o_custkey order by o_custkey) rown from 
# MAGIC (select o_custkey, year(o_orderdate) yd from samples.tpch.orders group by o_custkey, yd order by o_custkey) ) where rown = 1 order by o_custkey
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.tpch.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select o_custkey,max(year(o_orderdate)) ma, min(year(o_orderdate)) mi, ma-mi as diff
# MAGIC from samples.tpch.orders
# MAGIC group by o_custkey
# MAGIC order by diff desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Task 26 - Customer order frequency**

# COMMAND ----------

display(df_shipmode_joined_order_customer.limit(2))

# COMMAND ----------

df_shipmode_joined_order_customer.groupBy(["c_custkey","l_orderkey"]).agg(count("l_orderkey").alias("total_orders"), sum("revenue").alias("total_revenue"), avg("revenue").alias("average_revenue")).groupBy("c_custkey").agg(count("l_orderkey").alias("tota_orders"), sum("total_revenue").alias("tota_revenue")).orderBy(desc("tota_orders")).withColumn("rnk", row_number().over(Window.orderBy(desc("tota_orders")))).where("rnk <= 10").display()



# COMMAND ----------

