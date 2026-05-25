SELECT product, COUNT(*) AS total
FROM ecommerce_dataset.orders_ext
GROUP BY product
ORDER BY total DESC;