import psycopg2

# Establish a connection to the local PostgreSQL server
connection = psycopg2.connect(
    host="localhost",
    port="5432",
    database="employee",
    user="nadika",
    password='uxcam123'

)



cursor = connection.cursor()


# Create a sequence for the ID column
cursor.execute("CREATE SEQUENCE IF NOT EXISTS person_id_seq")


cursor.execute("""
CREATE TABLE IF NOT EXISTS person(
    id INT DEFAULT nextval('person_id_seq') PRIMARY KEY,
    
    first_name VARCHAR(23),
    last_name VARCHAR(23),
    age INTEGER
)
""")
connection.commit()




# Insert data
insert_query = """
INSERT INTO person (id, first_name, last_name, age)
VALUES (DEFAULT, %s, %s, %s)
"""
values = ("Mahims", "Dhakal", 1)
cursor.execute(insert_query, values)

# Commit the changes to the database
connection.commit()


cursor.execute("SELECT * from person")
rows=cursor.fetchall()
print(rows)

# cursor.execute("SELECT * FROM person")
# rows=cursor.fetchone()
# print(rows)

# #Insert with user input
# userinputs=input("Enter first name,last name and age")
# fname,lname,age=userinputs.split()

# cursor.execute("INSERT INTO person(first_name,last_name,age) VALUES (?,?,?)", (fname,lname,age))
# connection.commit()


# #Update data
# cursor.execute("UPDATE person SET age=35 WHERE first_name='nads' ")
# connection.commit()

# #Delete data
# cursor.execute("DELETE FROM person")
# connection.commit()


cursor.close()
connection.close()