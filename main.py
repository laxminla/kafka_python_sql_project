
## Python Kafka Producer ( Read data from employee File and write into Kafka topic)
# Python Kafka Consumer ( Read data from Kafka and write into Postgres table)


from producer import KafkaProducer
from consumer import KafkaConsumer
#from utils.database import Database
from database_class import Database
import logging
from utility import *
import configparser
import pandas as pd #module to read csv into dataframe

logging.basicConfig(filename='main.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')


class Main:

    def __init__(self):
        self.bootstrap = "localhost:9092"
        self.topic = "employee"


    def read_file(self, file):
        try:
            producer = KafkaProducer(self.bootstrap)
            with open(file, 'r') as emp:
                for line in emp:
                    #calling function produce_mess from producer.py and printing line by line
                    producer.produce_mess(self.topic, line.rstrip())
                    print("this is producer",line.rstrip())
            producer.flush()
        except Exception as e:
            logging.error(f'An error occurred while reading the config file: {str(e)}')
            print('err')
    def consume(self):
        try:
            #conn = Database("data/config.ini")
            #conn.connect()
            # group id is set by default in kafka config folder in consumer.properties file
            consumer = KafkaConsumer(self.bootstrap, 'test-consumer-group')
            consumer.subscribe([self.topic])
            for message in consumer.consume_message():
                #conn.insert_data("employee", message)
                print("this is consumer", message)
                message_items= message.split(",")
                print(type(message), message_items)

                db = Database()
                connection = db.create_server_connection(ipaddress, username, password, "NONE")

                print(connection)
                # set up the queries
                # CREATE THE DATABASE, FORM THE QUERY STRING TO CREATE A NEW DATABASE
                create_database_query = "CREATE DATABASE IF NOT EXISTS {dbname}".format(dbname=dbname)
                # CONNECT TO MYSQL USING THE QUERY STRING
                status = db.create_database(connection, create_database_query)
                # overwrite the connection variable to connect to a specific database in this case "testing"
                connection = db.create_server_connection(ipaddress, username, password, dbname)

                create_department_table_query = """
                CREATE TABLE IF NOT EXISTS {placeholder} (
                  dept_id INT PRIMARY KEY,
                  dept_name VARCHAR(40) NOT NULL
                  );
                 """.format(placeholder=table_name2)

                # THIS ACTUALLY CONNECTS TO MYSQL WITH THE QUERY STRING
                status2 = db.execute_query(connection, create_department_table_query)

                # QUERY STRING TO USE TO CREATE TABLE IF IT DOES NOT EXIST
                # employee_id,first_name,last_name,manager_name,salary,age,doj,dob
                # create the query string to create a new EMPLOYEES table with name of the value of variable table_name
                create_product_table_query = """
                    CREATE TABLE IF NOT EXISTS {placeholder} (
                      employee_id INT PRIMARY KEY,
                      first_name VARCHAR(40) NOT NULL,
                      last_name VARCHAR(40) NOT NULL,
                      manager_name VARCHAR(40) NOT NULL,
                      salary INT NOT NULL,
                      age INT NOT NULL,
                      doj VARCHAR(40) NOT NULL,
                      dob VARCHAR(40) NOT NULL,
                      dept_id int not null
                      );
                     """.format(placeholder=table_name)

                status = db.execute_query(connection, create_product_table_query)

                populate_table = """
                    INSERT INTO {table_name} VALUES ({employee_id},'{first_name}','{last_name}','{manager_name}',{salary},{doj},{age},{dob},{dept_id})
                    """
                #first_name, last_name, salary, age, doj, manager_name, employee_id, dob, dept_id
                populate_table_query = populate_table.format(table_name=table_name,
                                                             first_name=message_items[0], last_name=message_items[1],
                                                              salary=message_items[2],
                                                             age=message_items[3],
                                                             doj=message_items[4], manager_name=message_items[5],employee_id=message_items[6],dob=message_items[7], dept_id=message_items[8])
                # print(populate_table_query)
                status = db.execute_query(connection, populate_table_query)

            #conn.cursor.close()
        except Exception as e:
            logging.error(f'An error occurred while subscribing topic to mySQL: {str(e)}')
            print('consumer err',e)


if __name__ == '__main__':
    start_data = Main()
    #start_data.read_file("sample.csv")
    start_data.read_file(employeescsv)
    start_data.consume()