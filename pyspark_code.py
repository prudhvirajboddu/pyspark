from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
spark = SparkSession.builder.appName('Practice').getOrCreate()

# df1 = spark.read.csv("/content/olist_products_dataset.csv",inferSchema=True, header=True)

# df1.printSchema()

# df2 = spark.read.csv("/content/olist_order_items_dataset.csv",inferSchema=True, header=True)

# df2.createOrReplaceTempView("OrderItems")

# df5=spark.sql("SELECT product_id, COUNT(*) as product_count FROM OrderItems GROUP BY product_id")

# df6=df1.join(df5, df1['product_id'] == df5['product_id'], how='inner').select(col('product_category_name'),col('product_count'))
# df7=df6.orderBy(desc("product_count"))
# df7.show(10)


# df61 = spark.read.csv("/content/olist_customers_dataset.csv",inferSchema=True, header=True)
# df62 = spark.read.csv("/content/olist_geolocation_dataset.csv",inferSchema=True, header=True)

# df61.printSchema()
# df61.show(5)
# df62.printSchema()
# df62.show(5)

# df61.createOrReplaceTempView("Customers")

# df63=spark.sql("SELECT customer_zip_code_prefix, COUNT(customer_id) as customer_count FROM Customers GROUP BY customer_zip_code_prefix")
# df63.show(5)

















