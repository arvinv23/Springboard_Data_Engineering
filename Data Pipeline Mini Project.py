#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install mysql-connector-python


# In[2]:


import mysql.connector
import csv


# In[3]:


def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(user='root',
                                             password='Daringscape3#',
                                             host='localhost',
                                             port='3306',
                                             database='DE_database')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection


# In[4]:


class TicketSystem:
    def __init__(self, user, password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = None

    def get_db_connection(self):
        try:
            self.connection = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
        except Exception as error:
            print("Error while connecting to database:", error)
        return self.connection


# In[5]:


class TicketSystem:
    # ...

    def load_third_party(self, file_path_csv):
        try:
            connection = self.get_db_connection()
            cursor = connection.cursor()

            with open(file_path_csv, 'r') as file:
                csv_data = csv.reader(file)
                next(csv_data)  # Skip header row
                for row in csv_data:
                    sql = "INSERT INTO sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets) " \
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, row)

            connection.commit()
            cursor.close()
            connection.close()
            print("Data loaded successfully.")
        except Exception as error:
            print("Error while loading data:", error)


# In[12]:


class TicketSystem:
    def __init__(self, user, password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = None

    def get_db_connection(self):
        try:
            self.connection = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
        except Exception as error:
            print("Error while connecting to database:", error)
        return self.connection

    def load_third_party(self, file_path_csv):
        try:
            connection = self.get_db_connection()
            cursor = connection.cursor()

            with open(file_path_csv, 'r') as file:
                csv_data = csv.reader(file)
                next(csv_data)  # Skip header row
                for row in csv_data:
                    sql = "INSERT INTO sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets) " \
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, row)

            connection.commit()
            cursor.close()
            connection.close()
            print("Data loaded successfully.")
        except Exception as error:
            print("Error while loading data:", error)

    def query_popular_tickets(self):
        try:
            connection = self.get_db_connection()
            cursor = connection.cursor()

            # Get the date five years ago
            five_years_ago = datetime.now() - timedelta(days=365*5)
            sql = "SELECT event_name, SUM(num_tickets) as total_tickets_sold " \
                  "FROM sales " \
                  "WHERE date(trans_date) >= %s " \
                  "GROUP BY event_name " \
                  "ORDER BY total_tickets_sold DESC " \
                  "LIMIT 3"
            cursor.execute(sql, (five_years_ago,))
            records = cursor.fetchall()

            cursor.close()
            connection.close()
            return records
        except Exception as error:
            print("Error while querying data:", error)

    def display_popular_tickets(self):
        records = self.query_popular_tickets()
        if records:
            print("Here are the most popular tickets in the past 5 years:")
            for record in records:
                print(f"- {record[0]}")
        else:
            print("No popular tickets found.")

# Usage
ticket_system = TicketSystem(user='root', password='Daringscape3#', host='localhost', port='3306', database='DE_database')
ticket_system.load_third_party('third_party_sales_1.csv')
ticket_system.display_popular_tickets()

