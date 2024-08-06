import json
from pandas import json_normalize
import pandas as pd
import os
class Json_to_csv:
    def json_to_csv(self, folder_path, outpath, csv_name, max_length=35000):
        with open(folder_path, 'r') as f:
            data = json.load(f)

        flat_data = json_normalize(data)
        filtered_data = flat_data[flat_data.apply(lambda row: row.astype(str).str.len().sum() <= max_length, axis=1)]
        exceed_data = flat_data[flat_data.apply(lambda row: row.astype(str).str.len().sum() > max_length, axis=1)]
        if not exceed_data.empty:
            exceed_data = exceed_data.iloc[:, 0:8]
        result_data = pd.concat([filtered_data, exceed_data], ignore_index=True)

        output_file_path = os.path.join(outpath, csv_name)
        result_data.to_csv(output_file_path, index=False)
        print(f"Data has been saved to {output_file_path}")
