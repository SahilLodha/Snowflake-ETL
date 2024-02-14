import configparser  # For Reading the config file and extracting details
from pwinput import pwinput  # Taking Password as Input
# from snowflake.snowpark import Session  # Session generation via. Connection Parameter
from snowflake.connector import connect
from configparser import NoOptionError, NoSectionError  # Potential Exception Classes


class Configuration:
    def __init__(self, path):
        self.path: str = path
        self.config = configparser.ConfigParser()
        self.config.read(self.path)

    def get_connection_parameter(self) -> dict:
        """
        This Function Searches for a Config file and generates a connection parameter that is utilized during the generation
        of the connection/Session object. This doesn't store password and requires you to use a command prompt for entering
        the password.

        :return: dict - Connection Values
        """
        auth: dict = {}
        try:
            auth['user'] = self.config.get('DATABASE', 'username')
            auth['account'] = self.config.get('DATABASE', 'ACCOUNT_IDENTIFIER')
            auth['database'] = self.config.get('DATABASE', 'NAME')
            auth['password'] = self.config.get('DATABASE', 'PASSWORD')
            # auth['password'] = pwinput(prompt='Password: ')
            return auth
        except NoSectionError as exp:
            print(f'No {exp.section} found in {self.path}')
            exit(-1)
        except NoOptionError as exp:
            print(f"No option {exp.option} found under section {exp.section} in path {self.path}")
            exit(-1)

    def get_session(self):
        """
        It uses the above get_connection_parameter to retrieve the connection parameters and then initiates the connection
        request.

        # What you can Pass
        conn = snowflake.connector.connect(
        user='<your_username>',
        password='<your_password>',
        account='<your_account>',
        warehouse='<your_warehouse>',
        database='<your_database>',
        schema='<your_schema>'

        :return: Connection object.
        """
        connection_parameter = self.get_connection_parameter()
        return connect(
            user=connection_parameter['user'],
            password=connection_parameter['password'],
            account=connection_parameter['account'],
            database=connection_parameter['database']
        )

    def get_source_session(self):
        connection_parameter = self.get_connection_parameter()
        connection_parameter['database'] = self.config.get('DATABASE', 'SOURCE')
        return connect(
            user=connection_parameter['user'],
            password=connection_parameter['password'],
            account=connection_parameter['account'],
            database=connection_parameter['database']
        )

    def get_schema_name(self, environment: str):
        try:
            return self.config.get(f'{environment.upper()}', 'SCHEMA')
        except NoSectionError as err:
            print(f"No section {err.section} in path {self.path}")
        except NoOptionError as err:
            print(f"No option {err.option} in section {environment.upper()} in path {self.path}")
