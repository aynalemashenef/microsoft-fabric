#!/usr/bin/env python
# coding: utf-8

# ## gold_notebook
# 
# New notebook

# In[13]:


# Gold Layer notebook
# Shopping Mart Aggregation

from pyspark.sql.functions import *
orders_df = spark.read.format('parquet').load("Files/ShoppingMart_Silver_Orders/ShoppingMart_customers_orderdata")
reviews_df = spark.read.format('parquet').load("Files/ShoppingMart_Silver_Reviews/ShoppingMart_review")
social_df = spark.read.format('parquet').load("Files/ShoppingMart_Silver_Social_Media/ShoppingMart_social_media")
weblogs_df = spark.read.format('parquet').load("Files/ShoppingMart_Silver_Web_Logs/ShoppingMart_web_logs")
#display(social_df)

## create gold order file
orders_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Orders/ShoppingMart_customers_orderdata")

##KPI 1 - Aggregates web log data to measure engagement per user on each page and action
agg_weblogs_df = weblogs_df.groupBy("user_id", "page", "action").count()
agg_weblogs_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Web_Logs/ShoppingMart_web_logs")

### KPI2 : Aggregates unstructured social media data to track sentiment trends across different platforms
agg_social_df= social_df.groupBy("platform","sentiment" ).count()
agg_social_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Social_Media/ShoppingMart_social_media")


##KPI3: Aggregates product reviews to calculate the average rating per product
agg_reviews_df = reviews_df.groupBy("product_id").agg(avg("rating").alias("AvgRating"))
agg_reviews_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Reviews/ShoppingMart_review")


