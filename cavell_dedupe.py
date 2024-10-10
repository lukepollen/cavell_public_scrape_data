import sqlite3
import pandas as pd

# Function to drop duplicates in all user tables of an SQLite database, reset the index, and re-add the 'id' column
def drop_duplicates_in_db(db_path):

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    # Get a list of all user tables in the database, ignoring system tables (e.g., tables starting with sqlite_)
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    tables = pd.read_sql_query(query, conn)

    for table_name in tables['name']:
        # Read the table into a pandas DataFrame
        df = pd.read_sql_query(f"SELECT * FROM {table_name};", conn)

        # Drop the 'id' column if it exists
        if 'id' in df.columns:
            df = df.drop(columns=['id'])

        # Drop duplicate rows
        df_deduped = df.drop_duplicates()

        # Only proceed to reset index and re-add 'id' if duplicates were dropped
        if len(df_deduped) < len(df):
            # Reset the index and add a new 'id' column
            df_deduped = df_deduped.reset_index(drop=True)
            df_deduped['id'] = df_deduped.index + 1  # New 'id' starts from 1

            # Reorder columns so 'id' is the first column
            cols = ['id'] + [col for col in df_deduped.columns if col != 'id']
            df_deduped = df_deduped[cols]

            # Replace the original table with the deduplicated one
            df_deduped.to_sql(table_name, conn, if_exists='replace', index=False)

            print(f"Processed table '{table_name}': {len(df)} rows -> {len(df_deduped)} rows after removing duplicates.")

    # Close the connection
    conn.close()

# Example usage
db_path = 'output_database.db'  # Path to SQLite database
drop_duplicates_in_db(db_path)