import datetime
import pandas as pd

class OwnerProcessor:
    def __init__(self, logger):
        self.logger = logger

    def extract_attributes(self, row):
        data = {}

        data['owner_original_id'] = str(row['owner_id'])
        data['owner_first_name'] = str(row['first_name'])
        data['owner_last_name'] = str(row['last_name'])
        data['owner_email'] = str(row['email'])
        data['owner_address'] = str(row['address'])
        data['owner_city'] = str(row['city'])
        data['owner_postcode'] = str(row['postcode'])

        item = pd.Series(data=data)

        return item