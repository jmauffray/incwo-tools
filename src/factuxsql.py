from typing import List
import mysql.connector

# --- 1. Database Connection Details (Replace with your own) ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'factux'
}

TABLE_NAME = 'factux2_article' # The table you want IDs from
ID_COLUMN  = 'num'             # The name of the ID column

def get_list_of_ids_removed() -> List[str]:
    """Connects to the DB, fetches all IDs from a table, and returns them as a list."""
    connection = None  # Initialize to None
    cursor = None      # Initialize to None
    id_list = []

    try:
        # 2. Connect to the database
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # 3. Define and execute the SELECT query
        # We use ORDER BY to ensure the IDs are in a consistent order (optional)
        query = f"SELECT {ID_COLUMN}, 'actif' FROM {TABLE_NAME} WHERE actif = 'non' ORDER BY {ID_COLUMN}"
        cursor.execute(query)

        # 4. Fetch the results
        # fetchall() returns a list of tuples (e.g., [(1,), (2,), (3,)])
        results = cursor.fetchall()

        # Convert the list of tuples to a simple list of integers/strings
        id_list = [row[0] for row in results]

        print(f"✅ Successfully retrieved {len(id_list)} IDs.")
        return id_list

    except mysql.connector.Error as err:
        print(f"❌ Error: {err}")
        return []

    finally:
        # 5. Close the cursor and connection (Crucial step!)
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")

# --- Execution ---
if __name__ == "__main__":
    ids= get_list_of_ids_removed()
    if ids:
        # Print the first few IDs for verification
        print("\nFirst 5 IDs retrieved:")
        print(ids[:100])
        # Example of what you can do with the list:
        # print(f"\nTotal IDs: {len(ids)}")