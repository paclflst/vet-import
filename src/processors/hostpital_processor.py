import datetime
import pandas as pd

class HospitalProcessor():
    def __init__(self, logger):
        self.logger = logger

    def extract_attributes(self, row):
        data = {}

        data['hospital_original_id'] = str(row['hospital_id'])
        data['hospital_name'] = str(row['hospital'])

        item = pd.Series(data=data)

        return item
