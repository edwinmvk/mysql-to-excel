-- Create the database
DROP DATABASE IF EXISTS mysql_to_excel;
CREATE DATABASE mysql_to_excel;
USE mysql_to_excel;

-- Create Customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Products table
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT DEFAULT 0
);

-- Create Orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status ENUM('pending', 'processing', 'shipped', 'delivered') DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Create Order_Items table
CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    product_id INT,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Insert sample data into Customers
INSERT INTO customers (first_name, last_name, email) VALUES
    ('John', 'Doe', 'john.doe@email.com'),
    ('Jane', 'Smith', 'jane.smith@email.com'),
    ('Bob', 'Johnson', 'bob.johnson@email.com'),
    ('Alice', 'Williams', 'alice.williams@email.com'),
    ('Charlie', 'Brown', 'charlie.brown@email.com');

-- Insert sample data into Products
INSERT INTO products (product_name, description, price, stock_quantity) VALUES
    ('Laptop', 'High-performance laptop with 16GB RAM', 999.99, 50),
    ('Smartphone', '5G enabled smartphone with dual camera', 699.99, 100),
    ('Headphones', 'Wireless noise-canceling headphones', 199.99, 75),
    ('Tablet', '10-inch tablet with stylus support', 449.99, 30),
    ('Smartwatch', 'Fitness tracking smartwatch', 249.99, 60);

-- Insert sample data into Orders
INSERT INTO orders (customer_id, total_amount, status) VALUES
    (1, 1199.98, 'delivered'),
    (2, 699.99, 'shipped'),
    (3, 649.98, 'processing'),
    (4, 999.99, 'pending'),
    (5, 449.99, 'delivered');

-- Insert sample data into Order_Items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 999.99),
    (1, 3, 1, 199.99),
    (2, 2, 1, 699.99),
    (3, 3, 2, 199.99),
    (4, 1, 1, 999.99),
    (5, 4, 1, 449.99);
