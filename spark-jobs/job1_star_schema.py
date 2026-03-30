from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

POSTGRES_URL = "jdbc:postgresql://postgres:5432/spark_lab"
POSTGRES_PROPS = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Job1_StarSchema")
        .config("spark.jars", "/opt/spark/jars-extra/postgresql-42.7.3.jar")
        .getOrCreate()
    )


def read_raw_data(spark):
    df = (
        spark.read
        .jdbc(POSTGRES_URL, "mock_data", properties=POSTGRES_PROPS)
    )
    print(f"[INFO] Прочитано строк из mock_data: {df.count()}")
    return df


def build_dim_customer(raw_df):
    dim = (
        raw_df
        .select(
            F.col("sale_customer_id").alias("customer_id"),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed"
        )
        .dropDuplicates(["customer_id"])
        .orderBy("customer_id")
    )
    return dim


def build_dim_seller(raw_df):
    dim = (
        raw_df
        .select(
            F.col("sale_seller_id").alias("seller_id"),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code"
        )
        .dropDuplicates(["seller_id"])
        .orderBy("seller_id")
    )
    return dim


def build_dim_product(raw_df):
    dim = (
        raw_df
        .select(
            F.col("sale_product_id").alias("product_id"),
            "product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "pet_category",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date"
        )
        .dropDuplicates(["product_id"])
        .orderBy("product_id")
    )
    return dim


def build_dim_store(raw_df):
    dim = (
        raw_df
        .select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email"
        )
        .dropDuplicates(["store_name", "store_city"])
    )
    w = Window.orderBy("store_name", "store_city")
    dim = dim.withColumn("store_id", F.row_number().over(w))
    dim = dim.select(
        "store_id", "store_name", "store_location", "store_city",
        "store_state", "store_country", "store_phone", "store_email"
    )
    return dim


def build_dim_supplier(raw_df):
    dim = (
        raw_df
        .select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country"
        )
        .dropDuplicates(["supplier_name", "supplier_email"])
    )
    w = Window.orderBy("supplier_name")
    dim = dim.withColumn("supplier_id", F.row_number().over(w))
    dim = dim.select(
        "supplier_id", "supplier_name", "supplier_contact", "supplier_email",
        "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
    )
    return dim


def build_dim_time(raw_df):
    dates = (
        raw_df
        .select("sale_date")
        .dropDuplicates()
        .withColumn("parsed_date", F.to_date("sale_date", "M/d/yyyy"))
        .filter(F.col("parsed_date").isNotNull())
    )
    dim = (
        dates
        .withColumn("year", F.year("parsed_date"))
        .withColumn("month", F.month("parsed_date"))
        .withColumn("day", F.dayofmonth("parsed_date"))
        .withColumn("quarter", F.quarter("parsed_date"))
        .withColumn("day_of_week", F.dayofweek("parsed_date"))
        .withColumn("month_name", F.date_format("parsed_date", "MMMM"))
    )
    w = Window.orderBy("parsed_date")
    dim = dim.withColumn("time_id", F.row_number().over(w))
    dim = dim.select(
        "time_id", F.col("parsed_date").alias("full_date"),
        "year", "month", "day", "quarter", "day_of_week", "month_name",
        "sale_date"
    )
    return dim


def build_fact_sales(raw_df, dim_store, dim_supplier, dim_time):
    store_lookup = dim_store.select(
        F.col("store_id"),
        F.col("store_name").alias("s_name"),
        F.col("store_city").alias("s_city")
    )
    supplier_lookup = dim_supplier.select(
        F.col("supplier_id"),
        F.col("supplier_name").alias("sup_name"),
        F.col("supplier_email").alias("sup_email")
    )
    time_lookup = dim_time.select(
        F.col("time_id"),
        F.col("sale_date").alias("t_sale_date")
    )

    fact = (
        raw_df
        .join(store_lookup,
              (raw_df.store_name == store_lookup.s_name) &
              (raw_df.store_city == store_lookup.s_city),
              "left")
        .join(supplier_lookup,
              (raw_df.supplier_name == supplier_lookup.sup_name) &
              (raw_df.supplier_email == supplier_lookup.sup_email),
              "left")
        .join(time_lookup,
              raw_df.sale_date == time_lookup.t_sale_date,
              "left")
        .select(
            F.col("id").alias("sale_id"),
            F.col("sale_customer_id").alias("customer_id"),
            F.col("sale_seller_id").alias("seller_id"),
            F.col("sale_product_id").alias("product_id"),
            F.col("store_id"),
            F.col("supplier_id"),
            F.col("time_id"),
            F.col("sale_quantity"),
            F.col("sale_total_price"),
            F.to_date("sale_date", "M/d/yyyy").alias("sale_date")
        )
    )
    return fact


def write_to_postgres(df, table_name, mode="overwrite"):
    (
        df.write
        .mode(mode)
        .jdbc(POSTGRES_URL, table_name, properties=POSTGRES_PROPS)
    )


def main():
    spark = create_spark_session()

    raw_df = read_raw_data(spark)
    raw_df.cache()  # будем обращаться многократно

    dim_customer = build_dim_customer(raw_df)
    dim_seller = build_dim_seller(raw_df)
    dim_product = build_dim_product(raw_df)
    dim_store = build_dim_store(raw_df)
    dim_supplier = build_dim_supplier(raw_df)
    dim_time = build_dim_time(raw_df)

    fact_sales = build_fact_sales(raw_df, dim_store, dim_supplier, dim_time)

    write_to_postgres(dim_customer, "dim_customer")
    write_to_postgres(dim_seller, "dim_seller")
    write_to_postgres(dim_product, "dim_product")
    write_to_postgres(dim_store, "dim_store")
    write_to_postgres(dim_supplier, "dim_supplier")
    write_to_postgres(dim_time, "dim_time")
    write_to_postgres(fact_sales, "fact_sales")

    spark.stop()


if __name__ == "__main__":
    main()
