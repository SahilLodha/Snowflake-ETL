from ETL.utils import Configuration


class CreateTransformationEnvironment:
    def __init__(self, config: Configuration, prefix: str = 'TEMP_'):
        self.SCHEMA_NAME = config.get_schema_name('TEMP')
        self.table_prefix = prefix

    def generate(self, session, drop_schema: bool = True, drop_table: bool = False):
        query_list = [
            f'create schema if not exists {self.SCHEMA_NAME};',
        ]

        cursor = session.cursor()

        if not drop_schema:
            query_list.insert(0, f'DROP schema IF EXISTS {self.SCHEMA_NAME};')

        if drop_table is True:
            cursor.execute("SHOW TABLES;")
            for result in cursor.fetchall():
                table_name = result[1]
                if result[3] == self.SCHEMA_NAME:
                    drop_query = f'drop table if exists {self.SCHEMA_NAME}.{table_name};'
                    cursor.execute(drop_query)

        for query in query_list:
            cursor.execute(query)
            print(f'Query: {query} -> executed.')

        self.hierarchy_customer(cursor)
        self.hierarchy_product(cursor)
        self.hierarchy_store(cursor)
        self.hierarchy_time(cursor)
        self.fact_sales(cursor)

        cursor.close()

    def hierarchy_store(self, cursor):
        query_list: [str] = [
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_STR_RGN_CNY_LU (
                COUNTRY_ID NUMBER(38,0) NOT NULL,
                NAME VARCHAR(20),
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_STR_RGN_LU (
                REGION_ID NUMBER(38,0) NOT NULL,
                NAME VARCHAR(20),
                COUNTRY_ID NUMBER(38,0) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_STR_LU (
                STORE_ID NUMBER(38,0) NOT NULL,
                NAME VARCHAR(20),
                REGION_ID NUMBER(38,0) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );'''
        ]

        for query in query_list:
            cursor.execute(query)
            print(f'Query:\n {query}\nExecuted')

    def hierarchy_product(self, cursor) -> None:
        query_list = [
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_PROD_SUBCAT_CAT_LU (
                CATEGORY_ID NUMBER(38,0) NOT NULL,
                NAME VARCHAR(40) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_PROD_SUBCAT_LU (
                SUBCATEGORY_ID NUMBER(38,0) NOT NULL,
                NAME VARCHAR(40) NOT NULL,
                CATEGORY_ID NUMBER(38,0),
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_PROD_LU (
                PRODUCT_ID NUMBER(38,0) NOT NULL,
                NAME VARCHAR(40) NOT NULL,
                SUBCATEGORY_ID NUMBER(38,0),
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );'''
        ]

        for query in query_list:
            cursor.execute(query)
            print(f'Query:\n {query}\nExecuted')

    def hierarchy_time(self, cursor):
        query_list: [str] = [
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_DTIME_YR_LU (
                YEAR_ID NUMBER(38,0) NOT NULL,
                VALUE NUMBER(38,0),
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f"""create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_DTIME_HFYR_LU (
                HALF_YEAR_ID NUMBER(38,0) NOT NULL,
                VALUE NUMBER(38,0) NOT NULL,
                NAME VARCHAR(255),
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );""",
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_DTIME_QTR_LU (
                QUATER_ID NUMBER(38,0) NOT NULL,
                QUATER_NAME VARCHAR(255) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_DTIME_MTH_LU (
                MONTH_ID NUMBER(38,0) NOT NULL,
                NUMBER NUMBER(38,0) NOT NULL,
                NAME VARCHAR(40) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_DTIME_DOW_LU (
                DAY_OF_WEEK_ID NUMBER(38,0) NOT NULL,
                NAME VARCHAR(40) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_DTIME_DY_LU (
                DAY_ID NUMBER(38,0) NOT NULL,
                NUMBER NUMBER(38,0) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_DTIME_HR_LU (
                HOUR_ID NUMBER(38,0) NOT NULL,
                VALUE NUMBER(38,0) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );''',
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_DTIME_LU (
                DATE_TIME_ID NUMBER(38,0) NOT NULL,
                DATE_TIME TIMESTAMP_NTZ(9) NOT NULL,
                SECOND NUMBER(38,0),
                MINUTE NUMBER(38,0),
                HOUR_ID NUMBER(38,0),
                DAY_ID NUMBER(38,0),
                DAY_OF_WEEK NUMBER(38,0),
                MONTH_ID NUMBER(38,0),
                QUATER_ID NUMBER(38,0),
                HALF_YEAR_ID NUMBER(38,0),
                YEAR_ID NUMBER(38,0),
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );'''
        ]

        for query in query_list:
            cursor.execute(query)
            print(f'Query:\n {query}\nExecuted')

    def hierarchy_customer(self, cursor):
        query = f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_CSTM_LU (
                CUSTOMER_ID NUMBER(38,0),
                FIRST_NAME VARCHAR(255) NOT NULL,
                LAST_NAME VARCHAR(255) NOT NULL,
                MIDDLE_NAME VARCHAR(255),
                ADDRESS VARCHAR(255) NOT NULL,
                ACTIVE BOOLEAN DEFAULT TRUE,
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_START TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
                ACTIVE_END TIMESTAMP_NTZ(9)
            );'''

        cursor.execute(query)
        print(f'Query:\n {query}\nExecuted')

    def fact_sales(self, cursor):
        query = f"""create or replace  TABLE {self.SCHEMA_NAME}.{self.table_prefix}f_sales_dtl (
            sales_id      NUMBER(38,0),
            store_id      NUMBER(38,0),
            product_id    NUMBER(38,0),
            customer_id   NUMBER(38,0),
            date_time_id  NUMBER(38,0),
            quantity      int,
            amount        NUMBER(32, 2),
            discount      NUMBER(20, 2),
            inserted_date timestamp default current_timestamp,
            updated_date  timestamp default current_timestamp,
            active_start  timestamp default current_timestamp,
            active_end    timestamp default NULL
        );"""

        cursor.execute(query)
        print(f'Query:\n{query}\nExecuted.')
