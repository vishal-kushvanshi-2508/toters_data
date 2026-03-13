

from typing import List, Tuple

import mysql.connector # Must include .connector
import json



DB_CONFIG = {
    "host" : "localhost",
    "user" : "root",
    "password" : "actowiz",
    "port" : "3306",
    "database" : "toter_db"
}

def get_connection():
    try:
        ## here ** is unpacking DB_CONFIG dictionary.
        connection = mysql.connector.connect(**DB_CONFIG)
        ## it is protect to autocommit
        connection.autocommit = False
        return connection
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

def create_db():
    connection = get_connection()
    # connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS toter_db;")
    connection.commit()
    connection.close()
# create_db()


def create_table():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        query =  """
                CREATE TABLE IF NOT EXISTS product_detail(
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id INT,
                product_name VARCHAR(150),
                product_description VARCHAR(150),
                category_id INT  ,
                diet_info JSON ,
                product_IMG TEXT,
                measurement_unit VARCHAR(150) ,
                measurement_value VARCHAR(150) ,
                price INT ,
                price_usd DECIMAL(10,2) ,
                store_item_id INT ,
                store_id INT ,
                stock_level INT ,
                is_available BOOLEAN,
                currency VARCHAR(150) 
        ); """
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print("Table creation failed")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

batch_size_length = 100
def data_commit_batches_wise(connection, cursor, sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
    ## this is save data in database batches wise.
    batch_count = 0
    for index in range(0, len(sql_query_value), batch_size):
        batch = sql_query_value[index: index + batch_size]
        cursor.executemany(sql_query, batch)
        batch_count += 1
        connection.commit()
    return batch_count


def insert_data_in_table(list_data : list):
    connection = get_connection()
    cursor = connection.cursor()
    parent_sql = """INSERT INTO product_detail
                        (product_id, product_name, product_description, category_id, diet_info, product_IMG, measurement_unit, measurement_value, price, price_usd, store_item_id, store_id, stock_level, is_available, currency )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
    try:
        product_values = []
        for dict_data in list_data:
            product_values.append( (
                dict_data.get("product_id"),
                dict_data.get("product_name"),
                dict_data.get("product_description"),
                dict_data.get("category_id"),
                json.dumps(dict_data.get("diet_info")),
                dict_data.get("product_IMG"),
                dict_data.get("measurement_unit"),
                dict_data.get("measurement_value"),
                dict_data.get("price"),
                dict_data.get("price_usd"),
                dict_data.get("store_item_id"),
                dict_data.get("store_id"),
                dict_data.get("stock_level"),
                dict_data.get("is_available"),
                dict_data.get("currency")
            ))

        try:
            batch_count = data_commit_batches_wise(connection, cursor, parent_sql, product_values)
            print(f"Parent batches executed count={batch_count}")
        except Exception as e:
            print(f"batch can not. Error : {e} ")

        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        # print(f"Transaction failed, rolled back. Error: {e}")
        print("Transaction failed. Rolling back")
    except:
        print("except error raise ")
    finally:
        connection.close()

