# %%
'''# Importing necessary libraries 
import numpy as np # This is to... 
import pandas as pd # This is to extract and load the data 

# ------ Loading the JSON files --------
# All files will be loaded as a dataframe so it's easy to explore, clean, transform using various commands instead of manually parsing the data

# Branch info 
branchDF = pd.read_json('C:/Users/katherine.colon/Capstone Files/cdw_sapp_branch.json') # DataFrame that uses pandas to read the branch JSON file (had to use forward slashes in file path, backslashes were being read as escapes)
print(branchDF)

# Credit info
creditDF = pd.read_json('C:/Users/katherine.colon/Capstone Files/cdw_sapp_credit.json')
print(creditDF)

# Customer info
customerDF = pd.read_json('C:/Users/katherine.colon/Capstone Files/cdw_sapp_customer.json')
print(customerDF)'''

# %%
# Importing necessary libraries 
import findspark
findspark.init()
import pyspark 
from pyspark.sql import SparkSession # To extract, transform and load the data (ETL)

# Initializing the SparkSession
spark = SparkSession.builder.appName('Capstone').getOrCreate()

# ------ Extracting the JSON files --------
# All files will be loaded as a dataframe so it's easy to explore, clean, transform using various commands instead of manually parsing the data

# Reading branch info into a DataFrame
print("-------------------- \033[95mBranch DataFrame\033[0m ---------------------") # \033[95m = make this text bold(\033), and magenta ([95m), \033[0m = resets style (These are called ANSI escape codes)
branchDF = spark.read.json("C:/Users/katherine.colon/Capstone Files/cdw_sapp_branch.json",  multiLine=True) # File is array formatted, multiline=True tells Spark to parse the array 
branchDF.printSchema() # Will show me the data types for each column before transformation
branchDF.show() # Shows the table

# Reading credit info into a DataFrame
print("\n-------------------- \033[95mCredit DataFrame\033[0m --------------------")
creditDF = spark.read.json('C:/Users/katherine.colon/Capstone Files/cdw_sapp_credit.json', multiLine=True)
creditDF.printSchema()
creditDF.show()

# Reading customer info into a DataFrame
print("\n-------------------- \033[95mCustomer DataFrame\033[0m --------------------") 
customerDF = spark.read.json('C:/Users/katherine.colon/Capstone Files/cdw_sapp_customer.json', multiLine=True)
customerDF.printSchema()
customerDF.show()


# %%
# # ------ Transforming the JSON files --------



