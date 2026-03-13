

import gzip
import json
import re

def read_files_zip(file_path: str):
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            text = f.read()        # read file once
            text = re.sub(r'(\w)\n(\w)', r'\1\2', text)
            # convert to json
            data = json.loads(text)
            ## data insert into file ...
            # with open("toters_data.json", "w", encoding="utf-8") as file:
            #     json.dump(data, file, indent=4)
            yield data

    except Exception as e:
        print("Error:", e)

## extract data using jems path
from parsel import Selector
def extract_data(json_data):
    ### json_data = str type
    selector = Selector(json_data)
    data_information = selector.jmespath('data')
    toters_data = []
    for info in data_information:
        result = {}
        result['product_id'] = info.jmespath('id').get()
        result['product_name'] = info.jmespath('title').get()
        result['product_description'] = info.jmespath('description').get()
        result['category_id'] = info.jmespath('category_id').get()
        result['diet_info'] = json.dumps(info.jmespath('nutrition_facts.nutrition_info.diet_info').get() or {})
        result['product_IMG'] = info.jmespath('image').get()
        result['measurement_unit'] = info.jmespath('measurement_unit').get()
        result['measurement_value'] = info.jmespath('measurement_value').get()
        result['price'] = info.jmespath('original_price').get()
        result['price_usd'] = float(info.jmespath('original_price_usd').get())
        result['store_item_id'] = info.jmespath('store_item_id').get()
        result['store_id'] = info.jmespath('store_id').get()
        result['stock_level'] = info.jmespath('stock_level').get()
        result['is_available'] = info.jmespath('is_available').get()
        result['currency'] = info.jmespath('local_currency').get()
        toters_data.append(result)
    # print(json.dumps(toters_data, indent=4, ensure_ascii=False))
    # with open("final_extract_data.json", "w", encoding="utf-8") as file:
    #     json.dump(toters_data, file, indent=4)

    return toters_data

