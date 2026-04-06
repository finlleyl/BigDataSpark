from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.cores", "2")
        .getOrCreate()
    )


def read_star_schema(spark):
    tables = {}
    for name in ["fact_sales", "dim_customer", "dim_seller",
                 "dim_product", "dim_store", "dim_supplier"]:
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


# ============================
# Витрина 1: Продажи по продуктам
# ============================
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
        .join(prod, "product_id", "inner")
    )

    # 1. Топ-10 самых продаваемых продуктов
    top10 = (
        product_sales
        .select("product_id", "product_name", "product_category",
                "total_quantity_sold", "total_revenue", "num_sales")
        .orderBy(F.desc("total_quantity_sold"))
        .limit(10)
    )

    # 2. Общая выручка по категориям
    revenue_by_category = (
        product_sales
        .groupBy("product_category")
        .agg(
            F.sum("total_revenue").alias("category_revenue"),
            F.sum("total_quantity_sold").alias("category_quantity"),
            F.count("product_id").alias("num_products")
        )
        .orderBy(F.desc("category_revenue"))
    )

    # 3. Средний рейтинг и количество отзывов для каждого продукта
    avg_rating = (
        product_sales
        .select("product_id", "product_name", "product_category",
                "product_rating", "product_reviews",
                "total_quantity_sold", "total_revenue")
        .orderBy(F.desc("product_rating"))
    )

    return {
        "report_products_top10": top10,
        "report_products_revenue_by_category": revenue_by_category,
        "report_products_avg_rating": avg_rating,
    }


# ============================
# Витрина 2: Продажи по клиентам
# ============================
def build_report_customers(tables):
    fact = tables["fact_sales"]
    cust = tables["dim_customer"]

    customer_sales = (
        fact
        .groupBy("customer_id")
        .agg(
            F.sum("sale_total_price").alias("total_spent"),
            F.count("sale_id").alias("num_purchases"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
            F.sum("sale_quantity").alias("total_items_bought")
        )
        .join(cust, "customer_id", "inner")
    )

    # 1. Топ-10 клиентов по сумме покупок
    top10 = (
        customer_sales
        .select("customer_id", "customer_first_name", "customer_last_name",
                "customer_email", "total_spent", "num_purchases", "avg_check")
        .orderBy(F.desc("total_spent"))
        .limit(10)
    )

    # 2. Распределение по странам
    by_country = (
        customer_sales
        .groupBy("customer_country")
        .agg(
            F.count("customer_id").alias("num_customers"),
            F.sum("total_spent").alias("country_total_spent"),
            F.round(F.avg("avg_check"), 2).alias("country_avg_check")
        )
        .orderBy(F.desc("country_total_spent"))
    )

    # 3. Средний чек каждого клиента
    avg_checks = (
        customer_sales
        .select("customer_id", "customer_first_name", "customer_last_name",
                "avg_check", "num_purchases", "total_spent")
        .orderBy(F.desc("avg_check"))
    )

    return {
        "report_customers_top10": top10,
        "report_customers_by_country": by_country,
        "report_customers_avg_check": avg_checks,
    }


# ============================
# Витрина 3: Продажи по времени
# ============================
def build_report_time(tables):
    fact = tables["fact_sales"]

    # 1. Месячные и годовые тренды продаж
    monthly = (
        fact
        .withColumn("year", F.year("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .groupBy("year", "month")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("num_orders"),
            F.sum("sale_quantity").alias("total_items_sold")
        )
        .orderBy("year", "month")
    )

    # 2. Сравнение выручки за разные периоды (по годам и кварталам)
    quarterly = (
        fact
        .withColumn("year", F.year("sale_date"))
        .withColumn("quarter", F.quarter("sale_date"))
        .groupBy("year", "quarter")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("num_orders"),
            F.sum("sale_quantity").alias("total_items_sold")
        )
        .orderBy("year", "quarter")
    )

    # 3. Средний размер заказа по месяцам
    avg_order = (
        fact
        .withColumn("year", F.year("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .groupBy("year", "month")
        .agg(
            F.round(F.avg("sale_total_price"), 2).alias("avg_order_size"),
            F.round(F.avg("sale_quantity"), 2).alias("avg_items_per_order")
        )
        .orderBy("year", "month")
    )

    return {
        "report_time_monthly": monthly,
        "report_time_quarterly": quarterly,
        "report_time_avg_order": avg_order,
    }


# ============================
# Витрина 4: Продажи по магазинам
# ============================
def build_report_stores(tables):
    fact = tables["fact_sales"]
    store = tables["dim_store"]

    store_sales = (
        fact
        .groupBy("store_id")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("num_sales"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
            F.sum("sale_quantity").alias("total_items_sold")
        )
        .join(store, "store_id", "inner")
    )

    # 1. Топ-5 магазинов по выручке
    top5 = (
        store_sales
        .select("store_id", "store_name", "store_city", "store_country",
                "total_revenue", "num_sales", "avg_check")
        .orderBy(F.desc("total_revenue"))
        .limit(5)
    )

    # 2. Распределение продаж по городам и странам
    by_location = (
        store_sales
        .groupBy("store_country", "store_city")
        .agg(
            F.count("store_id").alias("num_stores"),
            F.sum("total_revenue").alias("location_revenue"),
            F.round(F.avg("avg_check"), 2).alias("location_avg_check")
        )
        .orderBy(F.desc("location_revenue"))
    )

    # 3. Средний чек магазина
    avg_checks = (
        store_sales
        .select("store_id", "store_name", "store_city",
                "avg_check", "total_revenue", "num_sales")
        .orderBy(F.desc("avg_check"))
    )

    return {
        "report_stores_top5": top5,
        "report_stores_by_location": by_location,
        "report_stores_avg_check": avg_checks,
    }


# ============================
# Витрина 5: Продажи по поставщикам
# ============================
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
            F.round(F.avg("product_price"), 2).alias("avg_product_price"),
            F.sum("sale_quantity").alias("total_items_sold")
        )
        .join(supplier, "supplier_id", "inner")
    )

    # 1. Топ-5 поставщиков по выручке
    top5 = (
        supplier_sales
        .select("supplier_id", "supplier_name", "supplier_country",
                "total_revenue", "num_sales")
        .orderBy(F.desc("total_revenue"))
        .limit(5)
    )

    # 2. Средняя цена товаров от поставщика
    avg_price = (
        supplier_sales
        .select("supplier_id", "supplier_name",
                "avg_product_price", "num_sales", "total_revenue")
        .orderBy(F.desc("avg_product_price"))
    )

    # 3. Распределение по странам поставщиков
    by_country = (
        supplier_sales
        .groupBy("supplier_country")
        .agg(
            F.count("supplier_id").alias("num_suppliers"),
            F.sum("total_revenue").alias("country_revenue"),
            F.round(F.avg("avg_product_price"), 2).alias("country_avg_price")
        )
        .orderBy(F.desc("country_revenue"))
    )

    return {
        "report_suppliers_top5": top5,
        "report_suppliers_avg_price": avg_price,
        "report_suppliers_by_country": by_country,
    }


# ============================
# Витрина 6: Качество продукции
# ============================
def build_report_quality(tables):
    fact = tables["fact_sales"]
    prod = tables["dim_product"]

    product_stats = (
        fact
        .groupBy("product_id")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.count("sale_id").alias("num_sales")
        )
        .join(prod, "product_id", "inner")
    )

    # 1. Продукты с наивысшим и наименьшим рейтингом (топ-10 + антитоп-10)
    best_worst = (
        product_stats
        .select("product_id", "product_name", "product_category",
                "product_brand", "product_rating", "total_revenue")
        .orderBy(F.desc("product_rating"))
    )

    # 2. Корреляция рейтинга и объёма продаж
    rating_vs_sales = (
        product_stats
        .select("product_id", "product_name", "product_rating",
                "total_quantity_sold", "total_revenue")
        .orderBy(F.desc("total_quantity_sold"))
    )

    # 3. Продукты с наибольшим кол-вом отзывов
    most_reviews = (
        product_stats
        .select("product_id", "product_name", "product_category",
                "product_reviews", "product_rating", "total_revenue")
        .orderBy(F.desc("product_reviews"))
    )

    return {
        "report_quality_rating": best_worst,
        "report_quality_rating_vs_sales": rating_vs_sales,
        "report_quality_most_reviews": most_reviews,
    }


def main():
    spark = create_spark_session()
    tables = read_star_schema(spark)

    builders = [
        build_report_products,
        build_report_customers,
        build_report_time,
        build_report_stores,
        build_report_suppliers,
        build_report_quality,
    ]

    for builder in builders:
        for name, df in builder(tables).items():
            write_to_clickhouse(df, name)

    spark.stop()


if __name__ == "__main__":
    main()