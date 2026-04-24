DROP DATABASE IF EXISTS binlog_traffic_demo;
CREATE DATABASE binlog_traffic_demo;
USE binlog_traffic_demo;

-- =========================
-- 1. Customers
-- =========================
CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL UNIQUE,
    phone VARCHAR(30),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- =========================
-- 2. Products
-- =========================
CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock_qty INT NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- =========================
-- 3. Orders
-- =========================
CREATE TABLE orders (
    order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status ENUM('NEW','PAID','SHIPPED','CANCELLED') NOT NULL DEFAULT 'NEW',
    total_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

-- =========================
-- 4. Order Items
-- =========================
CREATE TABLE order_items (
    order_item_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    line_total DECIMAL(12,2) NOT NULL,
    CONSTRAINT fk_items_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_items_product
        FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;

-- =========================
-- 5. Payments
-- =========================
CREATE TABLE payments (
    payment_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    payment_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(12,2) NOT NULL,
    payment_method ENUM('CARD','CASH','TRANSFER') NOT NULL,
    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
) ENGINE=InnoDB;


-- =========================
-- Seed data
-- =========================
INSERT INTO customers (full_name, email, phone) VALUES
('Carlos Jimenez', 'carlos@example.com', '8888-1111'),
('Maria Rodriguez', 'maria@example.com', '8888-2222'),
('Ana Vargas', 'ana@example.com', '8888-3333'),
('Jose Mora', 'jose@example.com', '8888-4444'),
('Laura Castro', 'laura@example.com', '8888-5555');

INSERT INTO products (product_name, category, price, stock_qty) VALUES
('Coffee 250g', 'Groceries', 8.50, 100),
('Chocolate Bar', 'Snacks', 2.75, 200),
('Notebook', 'Office', 4.25, 150),
('USB Cable', 'Electronics', 6.99, 80),
('Water Bottle', 'Accessories', 12.00, 60),
('Mouse Pad', 'Office', 5.50, 120),
('Headphones', 'Electronics', 24.99, 40),
('Tea Box', 'Groceries', 7.25, 90);


DELIMITER $$

CREATE PROCEDURE generate_sample_orders(IN p_order_count INT)
BEGIN
    DECLARE v_counter INT DEFAULT 0;
    DECLARE v_customer_id INT;
    DECLARE v_product_id INT;
    DECLARE v_order_id BIGINT;
    DECLARE v_quantity INT;
    DECLARE v_unit_price DECIMAL(10,2);
    DECLARE v_line_total DECIMAL(12,2);
    DECLARE v_items_per_order INT;
    DECLARE v_item_counter INT;
    DECLARE v_total DECIMAL(12,2);

    WHILE v_counter < p_order_count DO

        SET v_customer_id = (
            SELECT customer_id
            FROM customers
            ORDER BY RAND()
            LIMIT 1
        );

        INSERT INTO orders (customer_id, status, total_amount)
        VALUES (v_customer_id, 'NEW', 0);

        SET v_order_id = LAST_INSERT_ID();
        SET v_items_per_order = FLOOR(1 + RAND() * 4);
        SET v_item_counter = 0;
        SET v_total = 0;

        WHILE v_item_counter < v_items_per_order DO

            SET v_product_id = (
                SELECT product_id
                FROM products
                ORDER BY RAND()
                LIMIT 1
            );

            SELECT price
            INTO v_unit_price
            FROM products
            WHERE product_id = v_product_id;

            SET v_quantity = FLOOR(1 + RAND() * 5);
            SET v_line_total = v_quantity * v_unit_price;
            SET v_total = v_total + v_line_total;

            INSERT INTO order_items (
                order_id,
                product_id,
                quantity,
                unit_price,
                line_total
            )
            VALUES (
                v_order_id,
                v_product_id,
                v_quantity,
                v_unit_price,
                v_line_total
            );

            UPDATE products
            SET stock_qty = stock_qty - v_quantity
            WHERE product_id = v_product_id;

            SET v_item_counter = v_item_counter + 1;
        END WHILE;

        UPDATE orders
        SET 
            total_amount = v_total,
            status = ELT(FLOOR(1 + RAND() * 3), 'NEW', 'PAID', 'SHIPPED')
        WHERE order_id = v_order_id;

        IF RAND() > 0.30 THEN
            INSERT INTO payments (
                order_id,
                amount,
                payment_method
            )
            VALUES (
                v_order_id,
                v_total,
                ELT(FLOOR(1 + RAND() * 3), 'CARD', 'CASH', 'TRANSFER')
            );
        END IF;

        SET v_counter = v_counter + 1;
    END WHILE;
END$$

DELIMITER ;


-- Example usage:
--CALL generate_sample_orders(100);

-- Check generated data
--SELECT COUNT(*) AS total_orders FROM orders;
--SELECT COUNT(*) AS total_order_items FROM order_items;
--SELECT COUNT(*) AS total_payments FROM payments;