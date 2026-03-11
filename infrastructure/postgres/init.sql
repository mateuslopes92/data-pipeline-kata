CREATE TABLE sales (
    id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100),
    salesman VARCHAR(100),
    amount NUMERIC,
    source VARCHAR(20),
    timestamp BIGINT
);

INSERT INTO sales (id, city, salesman, amount, source, timestamp) VALUES
('1', 'New York', 'Alice', 200, 'db', 1710000001),
('2', 'New York', 'Bob', 150, 'db', 1710000002),
('3', 'Chicago', 'Alice', 300, 'db', 1710000003),
('4', 'Chicago', 'John', 100, 'db', 1710000004),
('5', 'Miami', 'Alice', 400, 'db', 1710000005),
('6', 'Miami', 'Bob', 50, 'db', 1710000006),
('7', 'Los Angeles', 'Carol', 500, 'db', 1710000007),
('8', 'Los Angeles', 'Alice', 120, 'db', 1710000008),
('9', 'Dallas', 'John', 220, 'db', 1710000009),
('10', 'Dallas', 'Bob', 180, 'db', 1710000010),
('11', 'Seattle', 'Carol', 260, 'db', 1710000011),
('12', 'Seattle', 'Alice', 310, 'db', 1710000012),
('13', 'Boston', 'Bob', 140, 'db', 1710000013),
('14', 'Boston', 'John', 190, 'db', 1710000014),
('15', 'San Francisco', 'Carol', 420, 'db', 1710000015),
('16', 'San Francisco', 'Alice', 330, 'db', 1710000016),
('17', 'Denver', 'Bob', 210, 'db', 1710000017),
('18', 'Denver', 'Carol', 390, 'db', 1710000018),
('19', 'Atlanta', 'Alice', 275, 'db', 1710000019),
('20', 'Atlanta', 'John', 160, 'db', 1710000020);