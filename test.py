from dotenv import load_dotenv
import psycopg2
import socket
import os

# Load environment variables from .env
load_dotenv()

# Fetch variables
USER = os.getenv("USER_KEY")
PASSWORD = os.getenv("PASS_KEY")
HOST = os.getenv("HOST_KEY")
PORT = os.getenv("PORT_KEY")
DBNAME = os.getenv("DBNAME_KEY")


orig_getaddrinfo = socket.getaddrinfo
def getaddrinfo_ipv4(*args, **kwargs):
    return [info for info in orig_getaddrinfo(*args, **kwargs) if info[0] == socket.AF_INET]
socket.getaddrinfo = getaddrinfo_ipv4

# Connect to the database
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Connection successful!")
    
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    
    # Example query
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print("Current Time:", result)

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Connection closed.")

except Exception as e:
    print(f"Failed to connect: {e}")