CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    salesman VARCHAR(100),
    amount NUMERIC
);

INSERT INTO sales (city, salesman, amount) VALUES
('New York', 'Alice', 200),
('New York', 'Bob', 150),
('Chicago', 'Alice', 300),
('Chicago', 'John', 100),
('Miami', 'Alice', 400),
('Miami', 'Bob', 50);