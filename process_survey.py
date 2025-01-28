import base64
from datetime import datetime
from surveyxact_utility_functions2 import OracleTableExtractor, RowTransformer, CreateRespondent, DatabaseManager
from surveyxact_logging_config import configure_logger2, logging  # Adjust the module name accordingly.

# Replace the basicConfig setup with the following:
logger = configure_logger2('base_survey.log')

def get_configuration(auth_email, auth_password, survey_key):
    # Encode the email and password for authentication.
    auth = base64.b64encode(bytes(f'{auth_email}:{auth_password}', 'utf8')).decode('utf8')
    # Construct the URL for the specific survey.
    url = f"https://rest.survey-xact.dk/rest/surveys/{survey_key}/respondents"
    # Set up the headers for the HTTP request.
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'Authorization': 'Basic ' + auth
    }
    params = {'distributionTs': '1'}

    return url, headers, params

def extract_respondent_data():
    # Connect to the Oracle database and extract rows.
    extractor = OracleTableExtractor(
        user='YOUR_USERNAME',
        password='YOUR_PASSWORD',
        host='YOUR_HOST',
        port='YOUR_PORT',
        service_name='YOUR_SERVICE_NAME'
    )
    
    rows = extractor.fetch_rows('basesurvey_cpr_level')
    
    # Transform rows into dictionaries for further processing.
    transformer = RowTransformer()
    respondent_dicts = [transformer.transform_row(row) for row in rows]
    
    # Clean any string fields in the dictionaries.
    for r in respondent_dicts:
        r = {k: v.strip() if isinstance(v, str) else v for k, v in r.items()}
        
    return respondent_dicts

def process_base_respondents(respondent_dicts, survey_id, url, headers, params, jobname):

    db_manager = DatabaseManager(
        user="YOUR_USERNAME",
        password="YOUR_PASSWORD",
        host="YOUR_HOST",
        port="YOUR_PORT",
        service_name="YOUR_SERVICE_NAME"
    )
    uploader = CreateRespondent(db_manager)
    count = 1

    for r in respondent_dicts:
        respondent_dict = {
            'email': r['EMAIL'],
            'entrytyp': r['ENTRYTYPE'],
            'telco': r['TELCO'],
            'accounti': r['ACCOUNTID'],
            'firstnam': r['FIRSTNAME'],
            'lastnam': r['LASTNAME'],
            'zipcode': r['ZIPCODE'],
            'sex': r['SEX'],
            'age': r['AGE'],
            'kundeniv': r['KUNDENIVEAUPRIS'],
            'schannel': r['SALESCHANNEL'],
            'permissi': r['PERMISSION'],
            'gadato': r['FIRSTACTIVEDATE'],
            'salgsdat': r['SUBSCRIBEDATE'],
            'mix': r['MIX'],
            'voice': r['VOICE'],
            'fwa': r['FWA'],
            'mbb': r['MBB'],
            'phone': r['PHONE'],
            'permissi': r['PERMISSION']
        }
        
        # Insert respondent to SurveyXact
        response = uploader.insert_respondent(
            respondent_dict,
            jobname=jobname,
            url=url,
            headers=headers,
            params=params
        ) 

        response_json = response.json()
        statuscode = str(response.status_code)
        respondent_key = response_json.get('externalkey')
        string_json = str(response_json)
        create_date = response_json.get('createts')
        date_obj = datetime.strptime(create_date, '%Y-%m-%d %H:%M:%S')
        
        # set accountid from json
        accountid = respondent_dict['accounti']

        # Save the respondent key to oracle
        uploader.save_respondent_to_db(
            survey_id,
            accountid,
            respondent_key,
            string_json,
            date_obj,
            statuscode,
            jobname
        )
        count += 1
        print(count)
        
    # Once done with all operations:
    db_manager.disconnect()

def process_survey(survey_id, jobname):
    # Hardcoded authentication details.
    AUTH_EMAIL = 'YOUR_USERNAME'
    AUTH_PASSWORD = 'YOUR_PASSWORD'

    # Get survey-specific configurations.
    url, headers, params = get_configuration(AUTH_EMAIL, AUTH_PASSWORD, survey_id)

    # Extract data from the Oracle DB.
    respondent_dicts = extract_respondent_data()
    print(len(respondent_dicts))

    # Process and insert each respondent's data.
    process_base_respondents(respondent_dicts, survey_id, url, headers, params, jobname)

def main():
    # Process the ENTRY SURVEY.
    logger.info("Starting processing BASESURVEY")
    try:
        process_survey('1596625', 'BASESURVEY')
        logger.info("Completed processing BASESURVEY")
    except Exception as e:
        logger.error(str(e))
        raise e

if __name__ == "__main__":
    main()
