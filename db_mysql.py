import mysql.connector

def get_database_connection():
    # Connect to MySQL server without specifying a database
    initial_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="GC)d<2Xa"
    )
    cursor = initial_connection.cursor()

    # Database name to work with
    database_name = "customers"

    # Check if the database exists
    cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
    result = cursor.fetchone()

    # If the database does not exist, create it
    if not result:
        cursor.execute(f"CREATE DATABASE {database_name}")
        print(f"Database '{database_name}' created successfully.")
    else:
        print(f"Database '{database_name}' already exists.")

    # Close the initial connection and cursor
    cursor.close()
    initial_connection.close()

    # Return a new connection that includes the specified database
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="GC)d<2Xa",
        database=database_name
    )

# Example Usage
if __name__ == "__main__":
    # Get the connection and use it
    db_connection = get_database_connection()
    print("Connection established:", db_connection)
