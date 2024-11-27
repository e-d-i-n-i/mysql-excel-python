import csv
from db_mysql import get_database_connection
from datetime import datetime
import re
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Get the connection
connection = get_database_connection()

# Use the connection
cursor = connection.cursor()

# CSV file to be loaded
filename = "customers-100.csv"

# Map CSV headers to database columns
column_mapping = {
    "First Name": "FirstName",
    "Last Name": "LastName",
    "Company": "Company",
    "City": "City",
    "Country": "Country",
    "Phone 1": "Phone1",
    "Phone 2": "Phone2",
    "Email": "Email",
    "Subscription Date": "SubscriptionDate",
    "Website": "Website"
}

# Data cleaning function
def clean_phone_number(phone_number):
    # Remove non-numeric characters, except plus sign
    return re.sub(r'[^0-9+]', '', phone_number)

def validate_email(email):
    # Validate email using regex
    email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(email_regex, email) is not None

def clean_data(row):
    # Clean up each field in the row
    cleaned_row = {}

    # Clean phone numbers
    if row.get("Phone 1"):
        cleaned_row["Phone 1"] = clean_phone_number(row["Phone 1"])
    else:
        cleaned_row["Phone 1"] = None

    if row.get("Phone 2"):
        cleaned_row["Phone 2"] = clean_phone_number(row["Phone 2"])
    else:
        cleaned_row["Phone 2"] = None

    # Clean other fields (remove leading/trailing whitespaces)
    for key, value in row.items():
        if key not in ["Phone 1", "Phone 2"]:
            cleaned_row[key] = value.strip() if value else None

    # Clean the subscription date
    if cleaned_row.get("Subscription Date"):
        try:
            cleaned_row["Subscription Date"] = datetime.strptime(cleaned_row["Subscription Date"], "%d/%m/%Y").strftime(
                "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format in row: {row}")

    # Validate email format
    if cleaned_row.get("Email") and not validate_email(cleaned_row["Email"]):
        raise ValueError(f"Invalid email format in row: {row}")

    return cleaned_row

# Bulk insert function
def bulk_insert(cursor, sql_insert, insert_batch):
    try:
        cursor.executemany(sql_insert, insert_batch)
        connection.commit()
        logging.info(f"Successfully inserted {len(insert_batch)} rows.")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting batch: {err}")

try:
    with open(filename, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=',')  # Adjust delimiter if necessary
        logging.info("CSV Headers: %s", csvreader.fieldnames)

        insert_batch = []
        batch_size = 1000  # Number of rows to insert at once

        # Process each row in the CSV file
        for row in csvreader:
            logging.info("Processing row: %s", row)

            # Clean the data
            cleaned_row = clean_data(row)

            # Extract relevant columns based on mapping
            values = [cleaned_row.get(key, None) for key in column_mapping.keys()]

            # Insert into the 'customers' table
            sql_insert = f"""
                INSERT INTO customers (
                    {', '.join(column_mapping.values())}
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            insert_batch.append(values)

            # Insert the batch when it reaches the batch size
            if len(insert_batch) >= batch_size:
                bulk_insert(cursor, sql_insert, insert_batch)
                insert_batch = []  # Reset the batch

        # Insert any remaining rows
        if insert_batch:
            bulk_insert(cursor, sql_insert, insert_batch)

        logging.info("CSV data has been successfully inserted into the 'customers' table.")

except FileNotFoundError:
    logging.error(f"Error: The file '{filename}' was not found.")
except ValueError as e:
    logging.error(f"Error: {e}")
except csv.Error as e:
    logging.error(f"Error processing CSV file: {e}")
except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    # Close resources
    cursor.close()
    connection.close()
