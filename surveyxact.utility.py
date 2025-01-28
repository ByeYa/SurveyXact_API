import requests
import logging
from datetime import datetime
import cx_Oracle
import base64
import json
import xmltodict
from analytics_bi_python.utility_functions import fetch_login
from surveyxact_logging_config import configure_logger2
from datetime import datetime

logger = configure_logger2('basesurvey_logger.log')

class DatabaseManager:
    def __init__(self, user: str, password: str, host: str, port: str, service_name: str):
        dsn = cx_Oracle.makedsn(host=host, port=port, service_name=service_name)
        self.connection = None
        self.cursor = None
        try:
            self.connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
            self.cursor = self.connection.cursor()
        except cx_Oracle.DatabaseError as e:
            print(f"Database connection error: {e}")
            raise e

    def disconnect(self):
        try:
            self.cursor.close()
        except:
            pass
        try:
            self.connection.close()
        except:
            pass


class OracleTableExtractor:
    """
    This class is responsible for connecting to Oracle, fetching rows
    from a specified table, and then disconnecting when it's no longer in use.
    """

    def __init__(self, user: str, password: str, host: str, port: str, service_name: str):
        """
        Initializes the OracleTableExtractor with the login details provided.

        Parameters:
        user: oracle user
        password: surveyxact password
        host: db host
        port: db port
        service_name: db servicename
        """

        dsn = cx_Oracle.makedsn(host=host, port=port, service_name=service_name)

        try:
            self.connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
            self.cursor = self.connection.cursor()
        except cx_Oracle.DatabaseError as e:
            logger.error(f'Database connection error: {e}')
            raise e

    def fetch_rows(self, table_name: str):
        """
        Fetches all rows from the given table in the Oracle database.

        Parameters:
        table_name (str): The name of the table to fetch rows from.

        Returns:
        list: A list of dictionaries representing each row in the table.
        """

        try:
            query = f"SELECT * FROM {table_name}"
            self.cursor.execute(query)
            columns = [column[0] for column in self.cursor.description]
            rows = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return rows
        except Exception as e:
            logger.error(f'Error fetching rows from table {table_name}: {e}')
            raise e

    def fetch_column(self, table_name: str, column_name: str):
        """
        Fetches all values from a specific column in the given table in the Oracle database.

        Parameters:
        table_name (str): The name of the table to fetch values from.
        column_name (str): The name of the column to fetch values from.

        Returns:
        list: A list of values representing each entry in the specified column.
        """

        try:
            query = f"SELECT {column_name} FROM {table_name}"
            self.cursor.execute(query)
        
            rows = [row[0] for row in self.cursor.fetchall()]

            logger.info(f'Successfully fetched values from column {column_name} in table {table_name}')
        
            return rows
        except Exception as e:
            logger.error(f'Error fetching values from column {column_name} in table {table_name}: {e}')
            raise e

    def fetch_column_respondent_status(self, table_name: str, column_name: str, conditions: dict = None):
        """
        Fetches all values from a specific column in the given table in the Oracle database.

        Parameters:
        table_name (str): The name of the table to fetch values from.
        column_name (str): The name of the column to fetch values from.
        conditions (dict, optional): A dictionary mapping column names to their expected values or complex condition.

        Returns:
        list: A list of values representing each entry in the specified column.
        """

        query = f"SELECT {column_name} FROM {table_name}"

        if conditions:
            condition_clauses = []
            for col, condition in conditions.items():
                if isinstance(condition, str) and condition.startswith("SQL:"):
                    condition_clauses.append(f"{col} {condition[4:]}")
                else:
                    condition_clauses.append(f"{col} = :{col}")
            query += f" WHERE {' AND '.join(condition_clauses)}"
    
        try:
            bind_variables = {k: v for k, v in conditions.items() if not (isinstance(v, str) and v.startswith("SQL:"))}
        
            self.cursor.execute(query, bind_variables)
    
            rows = [row[0] for row in self.cursor.fetchall()]
    
            return rows
        except Exception as e:
            logger.error(f'Error fetching values from column {column_name} in table {table_name}: {e}')
            raise e

    def fetch_column_respondent_status_NEW(self, table_name: str, column_name: str, conditions: dict = None):
        """
        Fetches all values from a specific column in the given table in the Oracle database.

        Parameters:
        table_name (str): The name of the table to fetch values from.
        column_name (str): The name of the column to fetch values from.
        conditions (dict, optional): A dictionary mapping column names to their expected values or complex condition.

        Returns:
        list: A list of values representing each entry in the specified column.
        """

        query = f"SELECT {column_name} FROM {table_name}"

        if conditions:
            condition_clauses = []
            for col, condition in conditions.items():
                if isinstance(condition, str) and condition.startswith("SQL:"):
                    condition_clauses.append(f"{col} {condition[4:]}")
                else:
                    condition_clauses.append(f"{col} = :{col}")
            query += f" WHERE {' AND '.join(condition_clauses)}"
    
        try:
            bind_variables = {k: v for k, v in conditions.items() if not (isinstance(v, str) and v.startswith("SQL:"))}
        
            self.cursor.execute(query, bind_variables)
    
            rows = [row[0] for row in self.cursor.fetchall()]
    
            return rows
        except Exception as e:
            logger.error(f'Error fetching values from column {column_name} in table {table_name}: {e}')
            raise e


    def __del__(self):
        try:
            self.cursor.close()
            self.connection.close()
        except Exception as e:
            logger.error(f'Error closing connection: {e}')
            raise e


class RowTransformer:
    """
    This class is responsible for transforming rows from the database into a specific format.
    """    

    def __init__(self):
        """
        Initializes the RowTransformer.
        """

    def transform_row(self, row: dict):
        """
        Transforms a given row by converting all values to strings.

        Parameters:
        row (dict): A dictionary representing a row in a database table.

        Returns:
        dict: The transformed row with all values as strings.
        """
        try:
            transformed = {key: str(value) for key, value in row.items()}
            return transformed
        except Exception as e:
            logger.error(f'Error transforming row {row}: {e}')
            raise e


class CreateRespondent:
    """
    This class is responsible for creating respondents, performing operations in SurveyXact, 
    and handling the relevant interactions with the database through the DatabaseManager.
    """

    def __init__(self, db_manager):
        """
        Initializes the CreateRespondent with provided data.

        Parameters:
        db_manager (DatabaseManager): An instance of DatabaseManager for database interactions.
        """
        self.db_manager = db_manager

    def insert_respondent(self, respondent_dict: dict, jobname: str, url, headers, params):
        """
        Inserts a respondent to the SurveyXact system and saves the key in Oracle.
        
        Parameters:
        jobname (str): entry or exit

        Returns:
        dict: The response from the SurveyXact system as a dictionary.
        """
        
        self.respondent_dict = respondent_dict
        self.url = url
        self.headers = headers
        self.params = params
        self.requestid = self.respondent_dict['accounti'] + datetime.now().strftime('%Y%m%d%H%M%S')

        try:
            self.respondent_dict['requesti'] = self.requestid
            payload = '&'.join([key+'='+value.encode().decode('latin1') for key, value in self.respondent_dict.items()])

            response = requests.request("POST", self.url, headers=self.headers, params=self.params, data=payload)
            statuscode = str(response.status_code)

            if response.status_code != 200:
                logger.error("Failed to insert respondent. HTTP Status code: %s", response.status_code)
                logger.error("Response: %s", response.text)
                return None

            return response
        except Exception as e:
            logger.exception("Exception occurred during insert_respondent: %s", str(e))
            raise

    def save_respondent_to_db(self, survey_id: str, accountid: str, respondent_key: str, json_file: str, create_date: str, statuscode: str, jobname: str):
        """
        Saves a new respondent's data in the Oracle database via the DatabaseManager.

        Parameters:
        accountid (str): The account ID for the respondent.
        respondent_key (str): The respondent's key from the SurveyXact system.
        json_file (str): The surveyxact JSON data.
        create_date (str): The date when the respondent data was created.
        statuscode (str): The status code of the respondent's creation process.
        jobname (str): ENTRY OR EXIT identifier.
        """
    
        try:
            stmt = """INSERT INTO surveyxact.sx_respondent_log 
                    (ACCOUNTID, EXTERNALKEY, JOBNAME, JSON, SENDT, STATUSCODE, SURVEYID, REQUESTID, SURVEY_STATUS) 
                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)"""

            values_to_insert = [accountid, respondent_key, jobname, json_file, create_date, statuscode, survey_id, self.requestid, 'Not Answered']

            for i in range(3):
                try:
                    self.db_manager.cursor.execute(stmt, values_to_insert)
                    self.db_manager.connection.commit()
                    break
                except Exception as e:
                    if i+1 == 3:
                        raise e
                    else:
                        self.db_manager = DatabaseManager(
                            user="SURVEYXACT",
                            password="YOUR_PASSWORD",
                            host="YOUR_HOST",
                            port="YOUR_PORT",
                            service_name="YOUR_SERVICE_NAME"
                        )
                        continue
        except cx_Oracle.DatabaseError as e:
            print("Failed to add the respondent to the database. Error:", e)
            raise


class RespondentAnswers:
    """
        Provides functionalities to fetch respondents' answers from SurveyXact and upload these answers to Oracle.

        Parameters:
        sx_login_password (str): Login password for SurveyXact.
        respondent_key (str): Unique identifier for a respondent in SurveyXact.
        survey_id (str): Unique identifier for a survey in SurveyXact.
    """

    def __init__(self, sx_login_password: str, respondent_key: str, survey_id: str) -> None:
        self.sx_login_password = sx_login_password
        self.respondent_key = respondent_key
        self.survey_id = survey_id

    def get_answers(self):
        """
        Fetches respondent answers and survey questionnaire from SurveyXact API.

        Returns:
            list: A list of dictionaries containing respondents' answers.
        """
        url_answer = "https://rest.survey-xact.dk/rest/respondents/{}/answer".format(self.respondent_key)
        url_questionnaire = "https://rest.survey-xact.dk/rest/surveys/{}/questionnaire".format(self.survey_id)
        
        auth = base64.b64encode(bytes(self.sx_login_password, 'utf8')).decode('utf8')
        headers = {'Authorization': 'Basic ' + auth}

        response = requests.request("GET", url_answer, headers=headers)
        survey = requests.request("GET", url_questionnaire, headers=headers)

        survey_data = json.dumps(xmltodict.parse(survey.text), indent=4)
        json_data = json.loads(survey_data)

        questions_mapping = {}
        foreground_variables = json_data['questionnaire']['foreground']['variable']

        for item in foreground_variables:
            question_name = item['@name']
            question_text = item['text'].get('#text', '')
            choices = {}

            if 'varChoice' in item:
                for choice in item['varChoice']:
                    choice_value = choice['@value']
                    choice_text = choice['text'].get('#text', '')
                    choices[choice_value] = choice_text

            questions_mapping[question_name] = {'text': question_text, 'choices': choices}

        json_data = json.dumps(xmltodict.parse(response.text), indent=4)
        data = json.loads(json_data)

        answers = []

        respondent_id = data['respondentanswer']['@key']
        survey_id = self.survey_id

        for answer in data['respondentanswer']['answer'].get('valueSingle', []):
            question_name = answer['@name']
            question_text = questions_mapping.get(question_name, {}).get('text', 'Unknown Question')

            question_value = answer['choice']['@value']
            choice_text = questions_mapping.get(question_name, {}).get('choices', {}).get(question_value, 'Unknown Choice')

            text_answer = next(
                (item['#text'] for item in data['respondentanswer']['answer'].get('valueText', [])
                 if item['@name'] == question_name), None
            )

            if text_answer:
                answers.append({
                    'respondent_id': respondent_id,
                    'survey_id': survey_id,
                    'question_name': question_name,
                    'question_text': question_text,
                    'question_value': question_value,
                    'choice_text': text_answer,
                })
            else:
                answers.append({
                    'respondent_id': respondent_id,
                    'survey_id': survey_id,
                    'question_name': question_name,
                    'question_text': question_text,
                    'question_value': question_value,
                    'choice_text': choice_text
                })

        for answer in data['respondentanswer']['answer'].get('valueText', []):
            question_name = answer['@name']
            if not any(a['question_name'] == question_name for a in answers):
                question_text = questions_mapping.get(question_name, {}).get('text', 'Unknown Question')
                text_answer = answer.get('#text', None)
        
                if text_answer is None:
                    continue
        
                answers.append({
                    'respondent_id': respondent_id,
                    'survey_id': survey_id,
                    'question_name': question_name,
                    'question_text': question_text,
                    'question_value': None,
                    'choice_text': text_answer
                })

        return answers
