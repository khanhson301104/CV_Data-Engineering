import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys


def import_excel_to_mysql(excel_file, table_name, host, user, password, database, sheet_name=0):
    try:
        # Read Excel file
        print("Reading Excel file...")
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Create connection
        print("Connecting to MySQL database...")
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        # Create table using TEXT type instead of VARCHAR
        print("Creating table if not exists...")
        columns = df.columns
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        create_table_sql += ", ".join([f"`{col}` TEXT" for col in columns])
        create_table_sql += ") CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        cursor.execute(create_table_sql)

        # Insert data
        print("Inserting data...")
        for index, row in df.iterrows():
            try:
                # Replace NaN with None
                values = [None if pd.isna(x) else str(x) for x in row]
                placeholders = ', '.join(['%s'] * len(row))
                columns_str = ', '.join([f'`{col}`' for col in columns])
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                cursor.execute(insert_sql, values)

                # Print progress every 100 rows
                if (index + 1) % 100 == 0:
                    print(f"Inserted {index + 1} rows...")
                    conn.commit()  # Commit every 100 rows

            except Error as row_error:
                print(f"Error inserting row {index + 1}: {row_error}")
                print(f"Problematic data: {values}")
                continue

        # Final commit
        conn.commit()
        print(f"Successfully imported {len(df)} rows into table {table_name}")

    except Error as e:
        print(f"MySQL Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    # Database configuration
    config = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'socom1'
    }

    # Excel file and table details
    EXCEL_FILE = 'D:/SQL_Test/socom1.xlsx'  # Use forward slashes
    TABLE_NAME = 'socom1'

    # Run the import
    import_excel_to_mysql(
        excel_file=EXCEL_FILE,
        table_name=TABLE_NAME,
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        sheet_name=0
    )