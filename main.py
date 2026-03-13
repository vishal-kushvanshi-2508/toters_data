

from extract_data import *

import sys
import time

from store_data_database import *

FILE_PATH = "C:/Users/vishal.kushvanshi/PycharmProjects/product_data/toters_3680_-15753_-62974_1.html.gz"

def main():
    create_table()
    print("table and db create")
    for raw_dict in read_files_zip(FILE_PATH):
        result = extract_data(json.dumps(raw_dict))
        insert_data_in_table(list_data=result)

if __name__ == "__main__":
    start = time.time()
    main()

    end = time.time()
    print("time different  : ", end - start)

