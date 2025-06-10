# Import Libraries
import mysql.connector as dbconn # The connection
from mysql.connector import Error # Handles errors while connecting
from prettytable import PrettyTable # Puts the sql query outputs in a table format instead of a deafult tuple

# Connection function 
def connect():
    conn = None
    try:
        conn = dbconn.connect(
            host = 'localhost',
            database = 'creditcard_capstone',
            user = 'root',
            password = 'password')
        if conn.is_connected():
            print("Fancy seeing you here! You are now connected to the MySQL database! :)")
            return conn
    except Error as e:
        print("Uh-Oh! Failed to connect... tartar sauce:",e)

# Connect to DB using connection function
conn = connect()
if not conn:
    exit()
# Variable to call the cursor - to execute queries 
cursor = conn.cursor()

# Print result function using PrettyTable!
def result(cursor):
    results = cursor.fetchall()
    if not results:
        print("Hmm... seems no results were found.")
    else:
        table = PrettyTable()
        table.field_names= [i[0] for i in cursor.description] # Gets the column names
        for row in results:
            table.add_row(row)
        print(table)

# Modify DB function
def modify(cursor, conn, ssn, fieldInput, newValue):
# Reformatting user input to translate to column names 
    field = fieldInput.strip().lower()
# Allows only specific fields to be updated (column names) with aliased using a dictionary
    allowedFields = {
        "email": "CUST_EMAIL",
        "first name": "FIRST_NAME",
        "middle name": "MIDDLE_NAME",
        "last name": "LAST_NAME",
        "phone": "CUST_PHONE",
        "address": "FULL_STREET_ADDRESS",
        "street address": "FULL_STREET_ADDRESS",
        "city": "CUST_CITY",
        "state": "CUST_STATE",
        "zip": "CUST_ZIP",
        "zip code": "CUST_ZIP",
        "country": "CUST_COUNTRY",
        "ssn": "SSN",
        "credit card": "CREDIT_CARD_NO"}
# Checks if the reformatted field is valid 
    if field not in allowedFields:
        print(f"Invalid field: {fieldInput}")
        return
    columnField = allowedFields[field]
# Trying the query     
    try: 
        query = "UPDATE customer SET {} = %s WHERE ssn = %s;".format(columnField)
        cursor.execute(query, (newValue, ssn,))
        conn.commit() # Saves changes to DB 
        print(f"{fieldInput} updated for customer SSN: {ssn}.")
    except Error as e:
        print("Uh-Oh, that didnt work!:",e)
# Confirming the changes 
    confirmQuery = "SELECT * FROM customer WHERE ssn = %s"
    cursor.execute(confirmQuery,(ssn,))
    result(cursor)
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Welcome and Menu for cli
while True:
    print("\n------------ Welcome to Example Bank! ------------")
    print("\033[94mBecause even your test data deserves a good home!\033[0m")
    print("\n------------ Main Menu ------------")
    print("\033[96m1. Customer Details\033[0m")
    print("\033[96m2. Transaction Details\033[0m")
    print("\033[96m3. Exit\033[0m")
    print("-----------------------------------")
    userInput = input("\nPlease enter one of the above options: ").strip()

# If Statement for the main menu options 
    if userInput == '1':
        print("\n\033[95m------------ Welcome to the Customer Details! ------------\033[0m")
        print("\n1. Check existing account")
        print("2. Modify existing account")
        print("3. Generate monthly bill")
        print("4. Display transactions")
        print("5. Return Home")
        print("\033[95m------------------------------------------------------------\033[0m")
        cInput = input("\nPlease enter one of the above options: ").strip()

# If Statement for Customer Details Menu Options 
        if cInput == '1': # Check existing account
            # Parameterized query to avoid sql injection! 
            ssn = input("\nPlease enter the 9-digit Social Security Number (numbers only): ").strip() # Callable variable to get user response
            query = "SELECT * FROM customer WHERE SSN = %s" # SQL query to retrieve all customer details where the ssn matches 
            cursor.execute(query,(ssn,)) # Runs and executes query 
            print("\n\033[4;94mCustomer Account:\033[0m")
            result(cursor) # Result function to display results 


        elif cInput == '2': # Modifies existing account
         ssn = input("\nPlease enter the 9-digit Social Security Number (numbers only): ").strip()
         fieldInput = input("Please specify which field you would like to update (e.g., 'first name', 'email'): ")
         newValue = input(f"Please enter the new {fieldInput}: ")

         modify(cursor, conn, ssn, fieldInput, newValue) # function to modify the account

        elif cInput == '3': # Generates a monthly bill
            ccn = input("\nPlease enter the Credit Card Number: ").strip()
            month = input("Please Enter the Month(2 digit format like 01): ").strip()
            year = input("Please enter the Year(4 digit format like 1111): ").strip()
            query = "SELECT transaction_id AS 'Transaction ID', transaction_value AS Transactions, timeid AS Date FROM credit WHERE timeid LIKE %s AND CREDIT_CARD_NO = %s ORDER BY timeid"
            likePattern = f"{year}{month}%" # Gives me the format I need using user input for the place holder after LIKE
            cursor.execute(query,(likePattern,ccn,))
            print("\n\033[4;94mMonthly Bill:\033[0m")
            result(cursor)
            # TO SHOW TOTAL 
            totalQuery = "SELECT ROUND(SUM(transaction_value), 2) AS Total FROM credit WHERE timeid LIKE %s AND CREDIT_CARD_NO = %s"
            cursor.execute(totalQuery, (likePattern,ccn,))
            result(cursor)


        elif cInput == '4': # Searches between two dates 
            ssn = input("\nPlease enter the 9-digit Social Security Number (numbers only): ").strip()
            date1 = input("Please enter the first date (format: yyyyMMdd): ").strip()
            date2 = input("Please enter the second date (format: yyyyMMdd): ").strip()
            query = "SELECT timeid AS Date, transaction_value AS Transactions FROM credit WHERE cust_ssn = %s AND timeid BETWEEN %s and %s ORDER BY timeid desc;"
            cursor.execute(query, (ssn,date1,date2,))
            print(f"\n\033[4;94mTransactions between {date1} and {date2}:\033[0m")
            result(cursor)


        else:
            print("\nReturning Home...")


    elif userInput == '2':
        print("\n\033[95m------------ Welcome to the Transaction Details! ------------\033[0m")
        proceed = input("Here you can retrieve a list of transactions filtered by Zip Code, Month, and Year. Would you like to proceed? (Y/N): ").strip().upper()
        if proceed == 'Y':
            #INPUTS
            zipCode = input("\nPlease enter the zip code (5 digit format like 99999): ").strip()
            month = input("Please enter the Month (2 digit format like 01): ").strip()
            year = input("Please enter the Year (4 digit format like 1111):").strip()
            #QUERY
            query = "SELECT c.cust_zip AS 'Zip Code', cc.transaction_value AS 'Transactions', cc.timeid AS 'Date' " \
            "FROM customer c " \
            "JOIN credit cc ON c.ssn = cc.cust_ssn  " \
            "WHERE c.cust_zip = %s AND cc.timeid LIKE %s " \
            "ORDER BY cc.timeid desc;"
            #EXECUTE AND PRINT
            likePattern = f"{year}{month}%"
            cursor.execute(query,(zipCode,likePattern,))
            print(f"\n\033[4;94mTransactions in Zip Code\033[0m \033[4;93m{zipCode}\033[0m \033[4;94mduring\033[0m \033[4;93m{month}/{year}:\033[0m")
            result(cursor)


        else:
            print("\nReturning Home...")


    elif userInput == '3':
        print("\nFarewell human! See you next time :)")
        break


    else:
        print("\n\033[91mThat input just tanked the stock. Wanna try again?\033[0m")


