from db_mysql import get_database_connection

# Get the connection
connection = get_database_connection()

# Use the connection
cursor = connection.cursor()

# Define table creation SQL with appropriate data types
sql_create_table = """
CREATE TABLE IF NOT EXISTS customers (
    CustomerId INT AUTO_INCREMENT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Company VARCHAR(100),
    City VARCHAR(50),
    Country VARCHAR(50),
    Phone1 VARCHAR(20),
    Phone2 VARCHAR(20),
    Email VARCHAR(100),
    SubscriptionDate DATE,
    Website VARCHAR(100)
)
"""

# Execute the SQL to create the table if it does not exist
cursor.execute(sql_create_table)
print("Table 'customers' has been created or already exists.")

# Verify if the table was created successfully
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print("List of tables in the database:")
for table in tables:
    print(table[0])

# Close the connection when done
cursor.close()
connection.close()
