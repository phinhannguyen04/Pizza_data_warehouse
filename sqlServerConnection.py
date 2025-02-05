"""
Connects to a SQL database using pyodbc
"""
import pyodbc

class SqlServerConnection:
    DRIVER = ""
    SERVER = ""
    DATABASE = ""

    def __init__(self, driver: str, server: str, database: str):
        self.DRIVER = driver
        self.SERVER = server
        self.DATABASE = database

    def connection(self):
        connection = pyodbc.connect(
            f"Driver={self.DRIVER};"
            f"Server={self.SERVER};"
            f"Database={self.DATABASE};"
            "Trusted_Connection=yes;"
        )
        print('Connection establish')
        return connection

# Test