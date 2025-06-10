# Import Libraries 
import requests
import mysql.connector as dbconn

# Function to fetch the data 
def fetch_api():
    # API URL https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json
    api = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
    # requests.get(API URL)
    response = requests.get(api)
    # calculate status code 
    if response.status_code == 200:
        print(f"Data Successfully fetched. Status code: {response.status_code}")
        return response.json() # returning the parsed json data
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None
# Load it to the DB 
def load(data):
    try:
    #connect to MySQL
        conn = dbconn.connect(
            host = 'localhost',
            port = '3306',
            user = 'root',
            password = 'password',
            database = 'creditcard_capstone' # Database name
        )
        cursor = conn.cursor()
        # Using a SQL query to create a table if it doesn't exist.
        cursor.execute( # Defines the name of the table 'loan_data' and the schema
            "CREATE TABLE IF NOT EXISTS loan_data (\
            Application_ID VARCHAR(255) PRIMARY KEY,\
            Gender VARCHAR(255),\
            Married VARCHAR(255),\
            Dependents VARCHAR(255),\
            Education VARCHAR(255),\
            Self_Employed VARCHAR(255),\
            Credit_History INT,\
            Property_Area VARCHAR(255),\
            Income VARCHAR(255),\
            Application_Status VARCHAR(255))" ) 
        
        # Insert data into the table
        for loan_data in data:
            cursor.execute('''
                INSERT INTO loan_data (Application_ID, Gender, Married, Dependents, Education, Self_Employed, Credit_History, Property_Area, Income, Application_Status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (loan_data['Application_ID'], loan_data['Gender'], loan_data['Married'], loan_data['Dependents'], loan_data['Education'], loan_data['Self_Employed'], 
                loan_data['Credit_History'], loan_data['Property_Area'], loan_data['Income'], loan_data['Application_Status']))
        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print("Data loaded successfully to MySQL database.")
    except Exception as e:
        print(f"Error: {e}")
# putting it all together 
# Variable to call the function that fetches the data from the api
api_data = fetch_api()

    # if the data is fetched successfully, load it into MySQL database 
if api_data:
    load(api_data)
