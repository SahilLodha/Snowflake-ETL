from ETL.utils import Configuration


class CreateStageEnvironment:
    def __init__(self, config: Configuration, prefix='STG_'):
        self.SCHEMA_NAME = config.get_schema_name('STAGE')
        self.table_prefix = prefix

    def generate(self, session, drop_schema=True, drop_table=False):
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
        self.fact_sales(cursor)

        cursor.close()
        return

    def hierarchy_product(self, cursor) -> None:
        query_list = [
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_PROD_SUBCAT_CAT (
                ID NUMBER(38,0) NOT NULL,
                COUNTRY_DESC VARCHAR(256),
                INSERTED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT NULL
            );''',

            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_PROD_SUBCAT (
                ID NUMBER(38,0) NOT NULL,
                CATEGORY_ID NUMBER(38,0),
                SUBCATEGORY_DESC VARCHAR(256),
                INSERTED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT NULL
            );''',

            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_PROD (
                ID NUMBER(38,0) NOT NULL,
                SUBCATEGORY_ID NUMBER(38,0),
                PRODUCT_DESC VARCHAR(256),
                INSERTED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT NULL
            );'''
        ]

        for query in query_list:
            cursor.execute(query)
            print(f'Query:\n {query}\nExecuted')

    def hierarchy_store(self, cursor) -> None:
        query_list = [
            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_STORE_RGN_COUNTRY (
                COUNTRY_ID NUMBER(38,0),
                REGION_DESC VARCHAR(256),
                INSERTED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT NULL
            );''',

            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_STORE_RGN (
                COUNTRY_ID NUMBER(38,0),
                REGION_DESC VARCHAR(256),
                INSERTED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT NULL
            );''',

            f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_STORE (
                ID NUMBER(38,0) NOT NULL,
                REGION_ID NUMBER(38,0),
                STORE_DESC VARCHAR(256),
                INSERTED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT NULL
            );'''
        ]

        for query in query_list:
            cursor.execute(query)
            print(f'Query:\n {query}\nExecuted')

    def hierarchy_customer(self, cursor) -> None:
        query = f'''
            create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}D_CUST (
                ID NUMBER(38,0) NOT NULL,
                CUSTOMER_FIRST_NAME VARCHAR(256),
                CUSTOMER_MIDDLE_NAME VARCHAR(256),
                CUSTOMER_LAST_NAME VARCHAR(256),
                CUSTOMER_ADDRESS VARCHAR(256),
                INSERTED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT NULL
            );
        '''
        cursor.execute(query)

        print(f'Query:\n {query}\nExecuted')

    def fact_sales(self, cursor):
        query = f'''create or replace TABLE {self.SCHEMA_NAME}.{self.table_prefix}F_SALES_DTL (
            ID NUMBER(38,0) NOT NULL,
            STORE_ID NUMBER(38,0) NOT NULL,
            PRODUCT_ID NUMBER(38,0) NOT NULL,
            CUSTOMER_ID NUMBER(38,0),
            TRANSACTION_TIME TIMESTAMP_NTZ(9),
            QUANTITY NUMBER(38,0),
            AMOUNT NUMBER(20,2),
            DISCOUNT NUMBER(20,2),
            INSERTED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UPDATED_AT TIMESTAMP DEFAULT NULL
        );'''

        cursor.execute(query)
        print(f'Query:\n {query}\nExecuted')