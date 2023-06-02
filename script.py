import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime
from faker import Faker
import random

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
        
    def insert_dummy_data(self, n_records=1000):
        fake = Faker()
        start_time = datetime.utcnow()
        records = []

        for _ in range(n_records):
            first_name = fake.first_name()
            last_name = fake.last_name()
            age = random.randint(18, 65)
            records.append((first_name, last_name, age))

        insert_query = """
        INSERT INTO person (first_name, last_name, age)
        VALUES (%s, %s, %s)
        """
        self.cursor.executemany(insert_query, records)
        self.connection.commit()
        
        end_time = datetime.utcnow()
        print(f"Total time taken to insert {n_records} dummy records: {str(end_time - start_time)}") 
    
    def insert_data(self, first_name, last_name, age):
        start_time=datetime.utcnow()
        # Insert data
        insert_query = """
        INSERT INTO person (id, first_name, last_name, age)
        VALUES (DEFAULT, %s, %s, %s)
        """
        values = (first_name, last_name, age)
        self.cursor.execute(insert_query, values)
        self.connection.commit()
        end_time=datetime.utcnow()
        print(f"Total time taken to run insert_data function : {str(end_time - start_time)}")
    
    def select_all_data(self):
        start_time=datetime.utcnow()
        # Select all data
        self.cursor.execute("SELECT * from person")
        rows = self.cursor.fetchall()
        for row in rows:
            print(row)
        end_time=datetime.utcnow()
        print(f"Total time taken to run select_all_data function : {str(end_time - start_time)}")
    
    def update_data(self, first_name, new_age):
        start_time=datetime.utcnow()
        # Update data
        update_query = """
        UPDATE person
        SET age = %s
        WHERE first_name = %s
        """
        values = (new_age, first_name)
        self.cursor.execute(update_query, values)
        self.connection.commit()
        end_time=datetime.utcnow()
    
    def delete_all_data(self):
        start_time=datetime.utcnow()
        # Delete all data
        self.cursor.execute("DELETE FROM person")
        self.connection.commit()
        end_time=datetime.utcnow()
        print(f"Total time taken to run delete_all_data function : {str(end_time - start_time)}")
        
    def delete_data(self, first_name):
        start_time=datetime.utcnow()
        # Delete data
        delete_query = """
        DELETE FROM person
        WHERE first_name = %s
        """
        values = (first_name,)
        self.cursor.execute(delete_query, values)
        self.connection.commit()
        end_time=datetime.utcnow()
        print(f"Total time taken to run delete_data function : {str(end_time - start_time)}")
        
    def insert_into_another_table(self):
        
        # check if table exists
        self.cursor.execute("""
                            SELECT EXISTS (
                                SELECT 1
                                FROM information_schema.tables
                                WHERE table_name = 'person_copy'
                            )
                            """
                            )
        table_exists = self.cursor.fetchone()[0]    
        if not table_exists:
            self.cursor.execute("""
                                CREATE TABLE person_copy(
                                    id INT PRIMARY KEY,
                                    first_name VARCHAR(23),
                                    last_name VARCHAR(23),
                                    age INTEGER
                                )
                                """
                                )
            self.connection.commit()
            
        
        # Insert data into another table
        insert_query = """
        INSERT INTO person_copy (id, first_name, last_name, age)
        SELECT id, first_name, last_name, age
        FROM person
        """
        self.cursor.execute(insert_query)
        self.connection.commit()
        
    
    
    def close_connection(self):
        self.cursor.close()
        self.connection.close()


# Code that will only be executed when the script is run directly
# This code will not run if the script is imported as a module
if __name__ == "__main__":
    start_time=datetime.utcnow()
    db = PersonDatabase()
    # db.create_table()
    # db.insert_dummy_data(100)
    # db.insert_dummy_data(1000000)
    # db.insert_data("hi", "dumb", 10)
    # db.select_all_data()
    # db.update_data("hi", 35)
    # db.select_all_data()
    # # db.delete_all_data()
    # db.delete_data("hi")
    # db.select_all_data()
    db.insert_into_another_table()
    db.close_connection()
    end_time=datetime.utcnow()
    print(f"Total time taken to run whole script : {str(end_time - start_time)}")
    
