import pandas as pd
import mysql.connector
import sys
import os # Import os for better path handling

# --- Configuration ---
# 1. Update your MySQL connection details
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'alvin',
    'password': 'password', # Make sure to enter your password
    'database': 'local_cabinweb'
}

# 2. Update your table details
TABLE_NAME = "room_prices"
PRIMARY_KEY_COLUMN = "id"         # The key to match rows (from your image)
# If you get a 'UnicodeDecodeError', try changing this to 'latin1' or 'cp1252'
CSV_ENCODING = 'utf-8' 
# --- End of Configuration ---

def update_database():
    # 1. Check for command-line argument and debug flag
    
    print("MAKESURE: file is csv and all data is cast to number")
    if len(sys.argv) < 2:
        print("Error: Missing CSV file path.")
        print("Usage: python3 massUpdate.py <path_to_excel_csv>")
        print("Add 'debug' as a second argument to show executed SQL statements.")
        sys.exit(1)
        
    CSV_FILE_PATH = sys.argv[1]
    # Check if the optional 'debug' argument is provided
    DEBUG_MODE = len(sys.argv) > 2 and sys.argv[2].lower() == 'debug'
    
    if DEBUG_MODE:
        print("--- DEBUG MODE ENABLED ---")

    try:
        # 2. Read the CSV data
        # IMPORTANT: Added the 'encoding' parameter to handle files saved with non-standard codecs.
        df = pd.read_csv(CSV_FILE_PATH, encoding=CSV_ENCODING)
        df = df.rename(columns={"#": PRIMARY_KEY_COLUMN})
        
        # Check if primary key is in the dataframe
        if PRIMARY_KEY_COLUMN not in df.columns:
            print(f"Error: Primary key column '{PRIMARY_KEY_COLUMN}' not found in CSV.")
            print(f"Found columns: {list(df.columns)}")
            print("Please ensure the first column in your CSV is named 'id' or '#'.")
            return

        print(f"Loaded {len(df)} rows from {CSV_FILE_PATH} using encoding: {CSV_ENCODING}")

    except UnicodeDecodeError as e:
        print("--- DECODE ERROR ---")
        print("Error reading CSV file with current encoding.")
        print("The file likely uses a different character set (e.g., saved from Excel on Windows).")
        print(f"Please try changing the 'CSV_ENCODING' variable in the script from '{CSV_ENCODING}' to 'latin1' or 'cp1252'.")
        print(f"Original error: {e}")
        return
    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE_PATH}' was not found.")
        print("Please check the path and try again.")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    try:
        # 3. Connect to the MySQL database
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor()
        print("Successfully connected to the database.")

        # 4. Dynamically build the UPDATE query TEMPLATE
        # Get all column names from the CSV, except the primary key
        update_columns = [col for col in df.columns if col != PRIMARY_KEY_COLUMN]
        
        # Create the "SET col1 = %s, col2 = %s, ..." part of the query
        set_clause = ", ".join([f"{col} = %s" for col in update_columns])
        
        # Build the full query TEMPLATE
        sql_query = f"""
            UPDATE {TABLE_NAME}
            SET {set_clause}
            WHERE {PRIMARY_KEY_COLUMN} = %s
        """
        print("Query Template:", sql_query.strip())

        # 5. Loop through each row in the CSV and execute the update
        updated_count = 0
        failed_count = 0
        
        for index, row in df.iterrows():
            try:
                # Create the tuple of values for the query
                # Values for the SET clause
                values_to_set = [row[col] for col in update_columns]
                # Add the primary key value for the WHERE clause
                values_tuple = tuple(values_to_set + [row[PRIMARY_KEY_COLUMN]])
                print(f"values_tuple : {values_tuple}")

                
                
                # Execute the query securely using placeholders
                cursor.execute(sql_query, values_tuple)
                updated_count += 1
            except Exception as row_error:
                print(f"Error updating row with {PRIMARY_KEY_COLUMN} {row[PRIMARY_KEY_COLUMN]}: {row_error}")
                failed_count += 1

        # 6. Commit changes and close
        db.commit()
        print("----------------------------------------")
        print(f"Database update complete.")
        print(f"Successfully updated: {updated_count} rows")
        print(f"Failed to update: {failed_count} rows")
        print("----------------------------------------")

    except mysql.connector.Error as db_err:
        print(f"Database connection error: {db_err}")
    finally:
        # Ensure connection is closed
        if 'db' in locals() and db.is_connected():
            cursor.close()
            db.close()
            print("Database connection closed.")

if __name__ == "__main__":
    # 03012025
    # not use since it execute row by row, not save for database. use bulk update 
    # update_database()
