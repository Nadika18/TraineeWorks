import psycopg2
from dotenv import load_dotenv
import os

# Load the environment variables from .env file
load_dotenv()

# Get the PostgreSQL password from the environment variables
password = os.getenv("DB_PASSWORD")


class PersonDatabase:
    def __init__(self):
        self.connection = psycopg2.connect(
            host="localhost",
            port="5432",
            database="employee",
            user="nadika",
            password=password
        )
        self.cursor = self.connection.cursor()
    
    def create_table(self):
        # Create a sequence for the ID column
        self.cursor.execute("CREATE SEQUENCE IF NOT EXISTS person_id_seq")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS person(
            id INT DEFAULT nextval('person_id_seq') PRIMARY KEY,
            first_name VARCHAR(23),
            last_name VARCHAR(23),
            age INTEGER
        )
        """)
        self.connection.commit()
    
    def insert_data(self, first_name, last_name, age):
        # Insert data
        insert_query = """
        INSERT INTO person (id, first_name, last_name, age)
        VALUES (DEFAULT, %s, %s, %s)
        """
        values = (first_name, last_name, age)
        self.cursor.execute(insert_query, values)
        self.connection.commit()
    
    def select_all_data(self):
        # Select all data
        self.cursor.execute("SELECT * from person")
        rows = self.cursor.fetchall()
        for row in rows:
            print(row)
    
    def update_data(self, first_name, new_age):
        # Update data
        update_query = """
        UPDATE person
        SET age = %s
        WHERE first_name = %s
        """
        values = (new_age, first_name)
        self.cursor.execute(update_query, values)
        self.connection.commit()
    
    def delete_all_data(self):
        # Delete all data
        self.cursor.execute("DELETE FROM person")
        self.connection.commit()
        
    def delete_data(self, first_name):
        # Delete data
        delete_query = """
        DELETE FROM person
        WHERE first_name = %s
        """
        values = (first_name,)
        self.cursor.execute(delete_query, values)
        self.connection.commit()
    
    def close_connection(self):
        self.cursor.close()
        self.connection.close()


# Code that will only be executed when the script is run directly
# This code will not run if the script is imported as a module
if __name__ == "__main__":
    db = PersonDatabase()
    db.create_table()
    db.insert_data("hi", "dumb", 10)
    db.select_all_data()
    db.update_data("hi", 35)
    db.select_all_data()
    # db.delete_all_data()
    db.delete_data("hi")
    db.select_all_data()
    db.close_connection()