from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/spark_lab"
PG_PROPS = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:ch://clickhouse:8123/default"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "spark",
    "password": "spark123"
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Job2_Reports_ClickHouse")
        .config("spark.jars",
                "/opt/spark/jars-extra/postgresql-42.7.3.jar,"
                "/opt/spark/jars-extra/clickhouse-jdbc-0.6.0-all.jar")
        .getOrCreate()
    )


def read_star_schema(spark):
    tables = {}
    for name in ["fact_sales", "dim_customer", "dim_seller",
                 "dim_product", "dim_store", "dim_supplier", "dim_time"]:
        tables[name] = spark.read.jdbc(PG_URL, name, properties=PG_PROPS)
    return tables


def write_to_clickhouse(df, table_name):
    filled = df.na.fill("").na.fill(0)
    (
        filled.write
        .mode("overwrite")
        .option("createTableOptions",
                "ENGINE = MergeTree() ORDER BY tuple()")
        .jdbc(CH_URL, table_name, properties=CH_PROPS)
    )


def build_report_products(tables):
    fact = tables["fact_sales"]
    prod = tables["dim_product"]

    product_sales = (
        fact
        .groupBy("product_id")
        .agg(
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("num_sales")
        )
    )

    report = (
        product_sales
        .join(prod, product_sales.product_id == prod.product_id, "inner")
        .select(
            prod.product_id,
            prod.product_name,
            prod.product_category,
            prod.product_brand,
            "total_quantity_sold",
            "total_revenue",
            "num_sales",
            prod.product_rating,
            prod.product_reviews,
            prod.product_price
        )
        .withColumn(
            "rank_by_quantity",
            F.row_number().over(
                Window.orderBy(F.desc("total_quantity_sold"))
            )
        )
        .orderBy("rank_by_quantity")
    )
    return report

def build_report_customers(tables):
    fact = tables["fact_sales"]
    cust = tables["dim_customer"]

    customer_sales = (
        fact
        .groupBy("customer_id")
        .agg(
            F.sum("sale_total_price").alias("total_spent"),
            F.count("sale_id").alias("num_purchases"),
            F.avg("sale_total_price").alias("avg_check"),
            F.sum("sale_quantity").alias("total_items_bought")
        )
    )

    report = (
        customer_sales
        .join(cust, customer_sales.customer_id == cust.customer_id, "inner")
        .select(
            cust.customer_id,
            cust.customer_first_name,
            cust.customer_last_name,
            cust.customer_email,
            cust.customer_country,
            cust.customer_age,
            "total_spent",
            "num_purchases",
            F.round("avg_check", 2).alias("avg_check"),
            "total_items_bought"
        )
        .withColumn(
            "rank_by_spent",
            F.row_number().over(
                Window.orderBy(F.desc("total_spent"))
            )
        )
        .orderBy("rank_by_spent")
    )
    return report

def build_report_time(tables):
    fact = tables["fact_sales"]
    time = tables["dim_time"]

    fact_time = fact.join(time, "time_id", "inner")

    report = (
        fact_time
        .groupBy("year", "month", "month_name", "quarter")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("num_orders"),
            F.avg("sale_total_price").alias("avg_order_size"),
            F.sum("sale_quantity").alias("total_items_sold")
        )
        .withColumn("avg_order_size", F.round("avg_order_size", 2))
        .orderBy("year", "month")
    )
    return report

def build_report_stores(tables):
    fact = tables["fact_sales"]
    store = tables["dim_store"]

    store_sales = (
        fact
        .groupBy("store_id")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("num_sales"),
            F.avg("sale_total_price").alias("avg_check"),
            F.sum("sale_quantity").alias("total_items_sold")
        )
    )

    report = (
        store_sales
        .join(store, "store_id", "inner")
        .select(
            "store_id",
            "store_name",
            "store_city",
            "store_state",
            "store_country",
            "total_revenue",
            "num_sales",
            F.round("avg_check", 2).alias("avg_check"),
            "total_items_sold"
        )
        .withColumn(
            "rank_by_revenue",
            F.row_number().over(
                Window.orderBy(F.desc("total_revenue"))
            )
        )
        .orderBy("rank_by_revenue")
    )
    return report

def build_report_suppliers(tables):
    fact = tables["fact_sales"]
    supplier = tables["dim_supplier"]
    product = tables["dim_product"]

    supplier_sales = (
        fact
        .join(product, "product_id", "inner")
        .groupBy("supplier_id")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("num_sales"),
            F.avg("product_price").alias("avg_product_price"),
            F.sum("sale_quantity").alias("total_items_sold")
        )
    )

    report = (
        supplier_sales
        .join(supplier, "supplier_id", "inner")
        .select(
            "supplier_id",
            "supplier_name",
            "supplier_city",
            "supplier_country",
            "supplier_contact",
            "total_revenue",
            "num_sales",
            F.round("avg_product_price", 2).alias("avg_product_price"),
            "total_items_sold"
        )
        .withColumn(
            "rank_by_revenue",
            F.row_number().over(
                Window.orderBy(F.desc("total_revenue"))
            )
        )
        .orderBy("rank_by_revenue")
    )
    return report

def build_report_quality(tables):
    fact = tables["fact_sales"]
    prod = tables["dim_product"]

    product_sales = (
        fact
        .groupBy("product_id")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.count("sale_id").alias("num_sales")
        )
    )

    report = (
        product_sales
        .join(prod, "product_id", "inner")
        .select(
            "product_id",
            "product_name",
            "product_category",
            "product_brand",
            "product_rating",
            "product_reviews",
            "total_revenue",
            "total_quantity_sold",
            "num_sales"
        )
        .withColumn(
            "rank_best_rating",
            F.row_number().over(
                Window.orderBy(F.desc("product_rating"))
            )
        )
        .withColumn(
            "rank_worst_rating",
            F.row_number().over(
                Window.orderBy(F.asc("product_rating"))
            )
        )
        .withColumn(
            "rank_most_reviews",
            F.row_number().over(
                Window.orderBy(F.desc("product_reviews"))
            )
        )
        .orderBy("rank_best_rating")
    )
    return report


def main():
    spark = create_spark_session()
    tables = read_star_schema(spark)

    reports = {
        "report_products": build_report_products(tables),
        "report_customers": build_report_customers(tables),
        "report_time": build_report_time(tables),
        "report_stores": build_report_stores(tables),
        "report_suppliers": build_report_suppliers(tables),
        "report_quality": build_report_quality(tables),
    }

    for name, df in reports.items():
        write_to_clickhouse(df, name)

    spark.stop()


if __name__ == "__main__":
    main()
