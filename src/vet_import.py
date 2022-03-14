import sys
import os
import pandas as pd
from sqlalchemy import create_engine
from processors.procedure_processor import ProcedureProcessor
from processors.owner_processor import OwnerProcessor
from processors.hostpital_processor import HospitalProcessor
from processors.pet_processor import PetProcessor
from processors.visit_processor import VisitProcessor
from services.database_service import DatabaseService
from datetime import datetime
from utils.logging import get_logger

logger = get_logger('vet_import')

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "vet_import")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "import")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "password")

now = datetime.now()


def main(filename: str = r'modelling-1.xlsx'):
    try:

        logger.debug(f'Start import {filename}')
        df = pd.read_excel(filename)

        vp = VisitProcessor(logger)

        conn_string = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}'
        dbs = DatabaseService(conn_string, logger)

        df, infalid_df = vp.validate_raw_visit(df)
        if not infalid_df.empty:
            infalid_df = infalid_df.assign(file=filename, import_ts=now)
            dbs.write_table(infalid_df, 'error_vet_visit')

        if df.empty:
            logger.error(
                f'File "{filename}" did not produce any rows after validation')

        else:

            pro_proc = ProcedureProcessor(logger)
            own_proc = OwnerProcessor(logger)
            hos_proc = HospitalProcessor(logger)
            pet_proc = PetProcessor(logger)
            vis_proc = VisitProcessor(logger)

            logger.debug('Import dim_procedure')
            procedure_df = df.apply(
                lambda row: pro_proc.extract_attributes(row), axis=1)
            dbs.save_dimention_table(procedure_df, 'dim_procedure')

            logger.debug('Import dim_owner')
            owner_df = df.apply(
                lambda row: own_proc.extract_attributes(row), axis=1)
            dbs.save_dimention_table(owner_df, 'dim_owner')

            logger.debug('Import dim_owner')
            hospital_df = df.apply(
                lambda row: hos_proc.extract_attributes(row), axis=1)
            dbs.save_dimention_table(hospital_df, 'dim_hospital')

            logger.debug('Import dim_owner')
            pet_df = df.apply(
                lambda row: pet_proc.extract_attributes(row), axis=1)
            dbs.save_dimention_table(pet_df, 'dim_pet')

            logger.debug('Import dim_owner')
            visit_df = df.apply(
                lambda row: vis_proc.extract_attributes(row), axis=1)
            dbs.save_fact_table(visit_df, 'fact_visit')

            logger.debug(f'Finish import {filename}')
    except Exception as e:
        logger.error(f'Import failed for file {filename}: \n{e}')


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error(
            'Please specifiy exactly one parameter: file to import (xlsx)')
    else:
        main(sys.argv[1])
