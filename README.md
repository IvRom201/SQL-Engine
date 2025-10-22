# MiniSQL Engine (Java)

A lightweight console-based SQL engine implemented in **pure Java**, without JDBC or ORM.  
It supports SQL-like queries and commands for creating, modifying, and querying in-memory tables.

---

## Features

| Category | Commands |
|-----------|-----------|
| **DDL** | `CREATE TABLE`, `ALTER TABLE ADD COLUMN`, `DROP TABLE` |
| **DML** | `INSERT`, `UPDATE`, `DELETE` |
| **Queries** | `SELECT ... FROM ... [WHERE ...] [ORDER BY ...] [LIMIT ...]` |
| **Joins** | `JOIN <table1> <table2> ON column1=column2 [INNER|LEFT|RIGHT]` |
| **Aggregates** | `AGG table FUNC(column)` — `COUNT`, `MIN`, `MAX`, `SUM`, `AVG` |
| **Utilities** | `LOAD <table> FROM '<path>'`, `DESCRIBE`, `TABLES`, `HELP` |

---

## Architecture
<img width="498" height="620" alt="image" src="https://github.com/user-attachments/assets/172b4526-a25f-4453-a0b5-dc2095098fbf" />

---

## Usage

### Run the console
```bash
javac -d out $(find src -name "*.java")
java -cp out Console_Layer.ConsoleApp

### Example session
CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING, age INTEGER, active BOOLEAN);

INSERT INTO users(id, name, age, active) VALUES (1, 'Alice', 28, true);
INSERT INTO users(id, name, age, active) VALUES (2, 'Bob', 17, false);

SELECT * FROM users;
SELECT name, age FROM users WHERE age > 18 ORDER BY name LIMIT 5;

UPDATE users SET active = false WHERE id = 1;
DELETE FROM users WHERE age < 18;

ALTER TABLE users ADD COLUMN email STRING;

AGG users COUNT(*);

- Load data
LOAD users FROM 'users.csv';
LOAD orders FROM 'orders.csv';

-- Left join
JOIN users orders ON id=user_id LEFT;

-- Aggregations
AGG orders SUM(amount) WHERE status='PAID';
AGG orders AVG(amount);

### Implementation Highlights

Custom SQL parser written from scratch using regex and string logic.

Query execution via in-memory filtering and Java predicates.

Support for multiple data types: STRING, INTEGER, DOUBLE, BOOLEAN.

Fully extensible: you can add GROUP BY, DROP COLUMN, or RENAME TABLE easily.

Educational architecture — great for learning how databases work internally.

### Requirements
- Java 17 or later
- No external libraries required (pure Java SE)

