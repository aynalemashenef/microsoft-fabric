#!/usr/bin/env python
# coding: utf-8

# ## Silver_Notebook
# 
# New notebook

# In[1]:


# Silver Layer
# Ingesting data from bronze layer

from pyspark.sql.functions import *

df_customers = spark.read.format('csv').option("header",True).load("Files/ShoppingMart_Bronze_Customers")
#display(df_customers)
df_orders = spark.read.format('csv').option("header",True).load("Files/ShoppingMart_Bronze_Orders")
df_products = spark.read.format('csv').option("header",True).load("Files/ShoppingMart_Bronze_Products")
df_reviews = spark.read.json("Files/ShoppingMart_Bronze_Reviews")
df_social = spark.read.json("Files/ShoppingMart_Bronze_Social_Media")
df_weblogs = spark.read.json("Files/ShoppingMart_Bronze_Web_Logs")
#display(df_weblogs)

# Data Cleaning and Enriching

df_remove_null_orders = df_orders.dropna(subset = ["OrderID", "CustomerID", "ProductID", "OrderDate", "TotalAmount"])
df_cast_order_date = df_remove_null_orders.withColumn("OrderDate", to_date(col("OrderDate")))
#display(df_cast_order_date)

# JOIN ORDERS WITH PRODUCTS & CUSTOMERS

customers_order_product = df_cast_order_date\
    .join(df_customers, on = 'CustomerID', how = 'inner' )\
    .join(df_products, on = 'ProductID', how = 'inner')

# Wite Data to SILVER Layer

customers_order_product.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Orders/ShoppingMart_customers_orderdata")
df_reviews.write.mode('overwrite').parquet("Files/ShoppingMart_Silver_Reviews/ShoppingMart_review")
df_social.write.mode('overwrite').parquet("Files/ShoppingMart_Silver_Social_Media/ShoppingMart_social_media")
df_weblogs.write.mode('overwrite').parquet("Files/ShoppingMart_Silver_Web_Logs/ShoppingMart_web_logs")

