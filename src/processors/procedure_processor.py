import datetime
import pandas as pd


class ProcedureProcessor:
    def __init__(self, logger):
        self.logger = logger

    def extract_attributes(self, row):
        data = {}

        data['procedure_original_id'] = str(row['procedure_code'])
        data['procedure_desc'] = row['procedure_desc'] if row['procedure_desc'] and type(
            row['procedure_desc']) == str else None

        item = pd.Series(data=data)

        return item
