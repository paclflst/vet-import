from datetime import datetime
import pandas as pd

class PetProcessor:
    def __init__(self, logger):
        self.logger = logger

    def extract_attributes(self, row):
        data = {}

        dob = datetime.strptime(str(row['pet_dob']), "%d/%m/%Y").date(
        ) if row['pet_dob'] and not pd.isna(row['pet_dob']) else None

        data['pet_original_id'] = str(row['pet_id'])
        data['pet_name'] = str(row['pet_name'])
        data['pet_species'] = str(row['species'])
        data['pet_breed'] = str(row['breed'])
        data['pet_dob'] = dob

        item = pd.Series(data=data)

        return item
