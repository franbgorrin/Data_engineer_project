CREATE SCHEMA ecommerce_dataset OPTIONS(location="US");
CREATE OR REPLACE EXTERNAL TABLE ecommerce_dataset.orders_ext
OPTIONS (
  format = 'CSV',
  uris = ['gs://ecommerce-datalake-123/orders.csv']
);