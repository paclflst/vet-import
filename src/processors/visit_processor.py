from datetime import datetime
import pandas as pd
import re
import pandera as pa
from pandera import Column, DataFrameSchema
from pandera.errors import SchemaErrors


class VisitProcessor:
    def __init__(self, logger):
        self.logger = logger

    def extract_attributes(self, row):
        data = {}

        start_date = datetime.strptime(str(row['visit_start_date']), "%d/%m/%Y").date(
        ) if row['visit_start_date'] and not pd.isna(row['visit_start_date']) else None

        cost = re.search('[0-9.]+', str(row['visit_cost'])).group()

        data['visit_original_id'] = str(row['visit_id'])
        data['visit_start_date'] = start_date
        data['visit_end_date'] = str(row['visit_end_date'])
        data['visit_cost'] = cost
        data['procedure_original_id'] = str(row['procedure_code'])
        data['hospital_original_id'] = str(row['hospital_id'])
        data['owner_original_id'] = str(row['owner_id'])
        data['pet_original_id'] = str(row['pet_id'])

        item = pd.Series(data=data)

        return item

    def validate_raw_visit(self, df):
        schema = DataFrameSchema(
            {
                "visit_id": Column(str, coerce=True),
                "visit_start_date": Column(str),
                "visit_end_date": Column(datetime),
                "visit_cost": Column(str),
                "procedure_code": Column(str),
                "procedure_desc": Column(str, nullable=True),
                "hospital_id": Column(str, coerce=True),
                "hospital": Column(str, nullable=True),
                "owner_id": Column(str, coerce=True),
                "first_name": Column(str, nullable=True),
                "last_name": Column(str, nullable=True),
                "email": Column(str, nullable=True),
                "address": Column(str, nullable=True),
                "city": Column(str, nullable=True),
                "postcode": Column(str, nullable=True),
                "pet_id": Column(str, coerce=True),
                "pet_name": Column(str, nullable=True),
                "species": Column(str, nullable=True),
                "breed": Column(str, nullable=True),
                "pet_dob": Column(str, nullable=True)
            }
        )
        try:
            schema.validate(df, lazy=True)
            return df, df[0:0]
        except SchemaErrors as err:
            critical_error_df = err.failure_cases[err.failure_cases['check']
                                                  != 'not_nullable']
            if not critical_error_df.empty:
                self.logger.error(
                    f'Critical validation errors found: \n{critical_error_df}')
                valid_df = df[0:0]
                invalid_df = df
                return valid_df, invalid_df
            self.logger.warning(
                f'Following row level errors found: \n{err.failure_cases}')
            valid_df = df[~df.index.isin(err.failure_cases["index"])]
            invalid_df = df[df.index.isin(err.failure_cases["index"])]
            return valid_df, invalid_df
