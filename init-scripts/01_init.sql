CREATE TEMP TABLE mock_data_staging (
    id                    INTEGER,
    customer_first_name   VARCHAR(100),
    customer_last_name    VARCHAR(100),
    customer_age          INTEGER,
    customer_email        VARCHAR(255),
    customer_country      VARCHAR(100),
    customer_postal_code  VARCHAR(20),
    customer_pet_type     VARCHAR(50),
    customer_pet_name     VARCHAR(100),
    customer_pet_breed    VARCHAR(100),
    seller_first_name     VARCHAR(100),
    seller_last_name      VARCHAR(100),
    seller_email          VARCHAR(255),
    seller_country        VARCHAR(100),
    seller_postal_code    VARCHAR(20),
    product_name          VARCHAR(200),
    product_category      VARCHAR(100),
    product_price         NUMERIC(10, 2),
    product_quantity      INTEGER,
    sale_date             VARCHAR(20),
    sale_customer_id      INTEGER,
    sale_seller_id        INTEGER,
    sale_product_id       INTEGER,
    sale_quantity          INTEGER,
    sale_total_price      NUMERIC(12, 2),
    store_name            VARCHAR(200),
    store_location        VARCHAR(200),
    store_city            VARCHAR(100),
    store_state           VARCHAR(100),
    store_country         VARCHAR(100),
    store_phone           VARCHAR(50),
    store_email           VARCHAR(255),
    pet_category          VARCHAR(50),
    product_weight        NUMERIC(10, 2),
    product_color         VARCHAR(50),
    product_size          VARCHAR(20),
    product_brand         VARCHAR(100),
    product_material      VARCHAR(100),
    product_description   TEXT,
    product_rating        NUMERIC(3, 1),
    product_reviews       INTEGER,
    product_release_date  VARCHAR(20),
    product_expiry_date   VARCHAR(20),
    supplier_name         VARCHAR(200),
    supplier_contact      VARCHAR(200),
    supplier_email        VARCHAR(255),
    supplier_phone        VARCHAR(50),
    supplier_address      VARCHAR(200),
    supplier_city         VARCHAR(100),
    supplier_country      VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS mock_data (
    id                    SERIAL PRIMARY KEY,
    customer_first_name   VARCHAR(100),
    customer_last_name    VARCHAR(100),
    customer_age          INTEGER,
    customer_email        VARCHAR(255),
    customer_country      VARCHAR(100),
    customer_postal_code  VARCHAR(20),
    customer_pet_type     VARCHAR(50),
    customer_pet_name     VARCHAR(100),
    customer_pet_breed    VARCHAR(100),
    seller_first_name     VARCHAR(100),
    seller_last_name      VARCHAR(100),
    seller_email          VARCHAR(255),
    seller_country        VARCHAR(100),
    seller_postal_code    VARCHAR(20),
    product_name          VARCHAR(200),
    product_category      VARCHAR(100),
    product_price         NUMERIC(10, 2),
    product_quantity      INTEGER,
    sale_date             VARCHAR(20),
    sale_customer_id      INTEGER,
    sale_seller_id        INTEGER,
    sale_product_id       INTEGER,
    sale_quantity          INTEGER,
    sale_total_price      NUMERIC(12, 2),
    store_name            VARCHAR(200),
    store_location        VARCHAR(200),
    store_city            VARCHAR(100),
    store_state           VARCHAR(100),
    store_country         VARCHAR(100),
    store_phone           VARCHAR(50),
    store_email           VARCHAR(255),
    pet_category          VARCHAR(50),
    product_weight        NUMERIC(10, 2),
    product_color         VARCHAR(50),
    product_size          VARCHAR(20),
    product_brand         VARCHAR(100),
    product_material      VARCHAR(100),
    product_description   TEXT,
    product_rating        NUMERIC(3, 1),
    product_reviews       INTEGER,
    product_release_date  VARCHAR(20),
    product_expiry_date   VARCHAR(20),
    supplier_name         VARCHAR(200),
    supplier_contact      VARCHAR(200),
    supplier_email        VARCHAR(255),
    supplier_phone        VARCHAR(50),
    supplier_address      VARCHAR(200),
    supplier_city         VARCHAR(100),
    supplier_country      VARCHAR(100)
);

DO $$
DECLARE
    i INTEGER;
    filepath TEXT;
BEGIN
    FOR i IN 1..10 LOOP
        filepath := '/data/mock_data_' || i || '.csv';
        BEGIN
            EXECUTE format(
                'COPY mock_data_staging FROM %L WITH (FORMAT csv, HEADER true, DELIMITER %L, QUOTE %L)',
                filepath, ',', '"'
            );
            RAISE NOTICE 'Imported: %', filepath;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Skipped (not found or error): % - %', filepath, SQLERRM;
        END;
    END LOOP;
END $$;

INSERT INTO mock_data (
    customer_first_name, customer_last_name, customer_age, customer_email,
    customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
    customer_pet_breed, seller_first_name, seller_last_name, seller_email,
    seller_country, seller_postal_code, product_name, product_category,
    product_price, product_quantity, sale_date, sale_customer_id, sale_seller_id,
    sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
    store_city, store_state, store_country, store_phone, store_email, pet_category,
    product_weight, product_color, product_size, product_brand, product_material,
    product_description, product_rating, product_reviews, product_release_date,
    product_expiry_date, supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city, supplier_country
)
SELECT
    customer_first_name, customer_last_name, customer_age, customer_email,
    customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
    customer_pet_breed, seller_first_name, seller_last_name, seller_email,
    seller_country, seller_postal_code, product_name, product_category,
    product_price, product_quantity, sale_date, sale_customer_id, sale_seller_id,
    sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
    store_city, store_state, store_country, store_phone, store_email, pet_category,
    product_weight, product_color, product_size, product_brand, product_material,
    product_description, product_rating, product_reviews, product_release_date,
    product_expiry_date, supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city, supplier_country
FROM mock_data_staging;

DROP TABLE mock_data_staging;