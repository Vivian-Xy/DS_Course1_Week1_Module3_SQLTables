import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect("data.sqlite")

# -------------------------
# Step 1
# -------------------------
df_boston = pd.read_sql("""
SELECT e.firstName, e.lastName, e.jobTitle
FROM employees e
JOIN offices o
ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
""", conn)

# -------------------------
# Step 2
# -------------------------
df_zero_emp = pd.read_sql("""
SELECT o.officeCode, o.city
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
WHERE e.employeeNumber IS NULL;
""", conn)

# -------------------------
# Step 3
# -------------------------
df_employee = pd.read_sql("""
SELECT e.firstName, e.lastName, o.city, o.state
FROM employees e
LEFT JOIN offices o
ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName;
""", conn)

# -------------------------
# Step 4
# -------------------------
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o
ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactLastName;
""", conn)

# -------------------------
# Step 5
# -------------------------
df_payment = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
FROM customers c
JOIN payments p
ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC;
""", conn)

# -------------------------
# Step 6
# -------------------------
df_credit = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName,
       COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber, e.firstName, e.lastName
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC;
""", conn)

# -------------------------
# Step 7
# -------------------------
df_product_sold = pd.read_sql("""
SELECT p.productName,
       COUNT(od.orderNumber) AS numorders,
       SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od
ON p.productCode = od.productCode
GROUP BY p.productCode, p.productName
ORDER BY totalunits DESC;
""", conn)

# -------------------------
# Step 8
# -------------------------
df_total_customers = pd.read_sql("""
SELECT p.productName, p.productCode,
       COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od
ON p.productCode = od.productCode
JOIN orders o
ON od.orderNumber = o.orderNumber
GROUP BY p.productCode, p.productName
ORDER BY numpurchasers DESC;
""", conn)

# -------------------------
# Step 9
# -------------------------
df_customers = pd.read_sql("""
SELECT o.officeCode, o.city,
       COUNT(c.customerNumber) AS n_customers
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
LEFT JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode, o.city;
""", conn)

# -------------------------
# Step 10
# -------------------------
df_under_20 = pd.read_sql("""
WITH low_products AS (
    SELECT p.productCode
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY p.productCode
    HAVING COUNT(DISTINCT o.customerNumber) < 20
)

SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName,
       o.city, o.officeCode
FROM employees e
JOIN customers c
    ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN offices o
    ON e.officeCode = o.officeCode
JOIN orders ord
    ON c.customerNumber = ord.customerNumber
JOIN orderdetails od
    ON ord.orderNumber = od.orderNumber
JOIN low_products lp
    ON od.productCode = lp.productCode
""", conn)

# Close connection
conn.close()