# Importing necessary libraries 
import findspark
findspark.init()
import pyspark 
from pyspark.sql import SparkSession # To extract, transform and load the data (ETL)
# Importing built in function from pyspark for specific transformations
from pyspark.sql.functions import format_string, substring, concat, lpad, col, concat_ws, initcap, lower
# Importing schema editing libraries 
from pyspark.sql.types import StructType,StructField, IntegerType, StringType, TimestampType, DoubleType
# Initializing the SparkSession
spark = SparkSession.builder.appName('Capstone').getOrCreate()

# ------ Extracting the JSON files --------
# All files will be loaded as a dataframe so it's easy to explore, clean, transform using various commands instead of manually parsing the data

# Establishing schema AND reading branch info into a DataFrame 

# SCHEMA
branchSchema = StructType([
     StructField('BRANCH_CODE', IntegerType(), True),
     StructField('BRANCH_NAME', StringType(), True),
     StructField('BRANCH_STREET', StringType(), True),
     StructField('BRANCH_CITY', StringType(), True),
     StructField('BRANCH_STATE', StringType(), True),
     StructField('BRANCH_ZIP', StringType(), True),
     StructField('BRANCH_PHONE', StringType(), True),
     StructField('LAST_UPDATED', TimestampType(), True)
         ])

# EXTRACT
print("-------------------- \033[95mBranch DataFrame\033[0m ---------------------") # \033[95m = make this text bold(\033), and magenta ([95m), \033[0m = resets style (These are called ANSI escape codes)
branchDF = spark.read.schema(branchSchema).json("C:/Users/katherine.colon/CapstoneFiles/Data/cdw_sapp_branch.json",  multiLine=True) # File is array formatted, multiline=True tells Spark to parse the array 
print(branchDF.columns)
branchDF.printSchema() # Will show me the data types for each column before transformation
branchDF.show() # Shows the table

# Establishing schema AND reading credit info into a DataFrame

# SCHEMA
creditSchema = StructType([
     StructField('CREDIT_CARD_NO', StringType(), True),
     StructField('TIMEID', StringType(), True),
     StructField('CUST_SSN', IntegerType(), True),
     StructField('BRANCH_CODE', IntegerType(), True),
     StructField('TRANSACTION_TYPE', StringType(), True),
     StructField('TRANSACTION_VALUE', DoubleType(), True),
     StructField('TRANSACTION_ID', IntegerType(), True),
     StructField('YEAR', IntegerType(), True),
     StructField('MONTH', IntegerType(), True),
     StructField('DAY', IntegerType(), True)
         ])

print("\n-------------------- \033[95mCredit DataFrame\033[0m --------------------")
creditDF = spark.read.schema(creditSchema).json('C:/Users/katherine.colon/CapstoneFiles/Data/cdw_sapp_credit.json', multiLine=True)
print(creditDF.columns)
creditDF.printSchema()
creditDF.show()

# Establishing schema AND reading customer info into a DataFrame

# SCHEMA
customerSchema = StructType([
     StructField('SSN', IntegerType(), True),
     StructField('FIRST_NAME', StringType(), True),
     StructField('MIDDLE_NAME', StringType(), True),
     StructField('LAST_NAME', StringType(), True),
     StructField('CREDIT_CARD_NO', StringType(), True),
     StructField('FULL_STREET_ADDRESS', StringType(), True),
     StructField('CUST_CITY', StringType(), True),
     StructField('CUST_STATE', StringType(), True),
     StructField('CUST_COUNTRY', StringType(), True),
     StructField('CUST_ZIP', StringType(), True),
     StructField('CUST_PHONE', StringType(), True),
     StructField('CUST_EMAIL', StringType(), True),
     StructField('LAST_UPDATED', TimestampType(), True),
     StructField('STREET_NAME', StringType(), True),
     StructField('APT_NO', StringType(), True)
         ])

# EXTRACT
print("\n-------------------- \033[95mCustomer DataFrame\033[0m --------------------") 
customerDF = spark.read.schema(customerSchema).json('C:/Users/katherine.colon/CapstoneFiles/Data/cdw_sapp_customer.json', multiLine=True)
print(customerDF.columns)
customerDF.printSchema()
customerDF.show()


# ------ Transforming the JSON files --------

# Branch info transformations

branchDF.createOrReplaceTempView("Branch") # Creating or replacing(if already exists) the 'branch' table 

# If the source value is null load default (999999)
branchDF = branchDF.na.fill("999999", subset=["BRANCH_ZIP"])
branchDF.show()

# Change the format of phone number to (xxx)xxx-xxxx

branchDF = branchDF.withColumn( # Using the withColumn to replace the current column with the updated format
    "BRANCH_PHONE", # The column name (1st arg)
    format_string("(%s)%s-%s", # (2nd arg) formats the data. I'm specifiy how I want the data to be formatted with %s place holders for the string value
                  substring("BRANCH_PHONE",1,3), # Extracts from the Branch_Phone column. Starts at the 1st character and retrieves 3 chars (the len)
                  substring("BRANCH_PHONE",4,3), 
                  substring("BRANCH_PHONE",7,4)))
branchDF.show()

# Loading Edited Branch table

branchDF.select('BRANCH_CODE', 'BRANCH_NAME', 'BRANCH_STREET', 'BRANCH_CITY', 'BRANCH_STATE', 'BRANCH_ZIP', 'BRANCH_PHONE', 'LAST_UPDATED').write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.Branch") \
  .option("user", "root") \
  .option("password", "password") \
.option("header","false")\
.save()


# Credit Card Info Transformations 

creditDF.createOrReplaceTempView("Credit") # Creating the table

# Convert Day, Month, and YEAR into a TIMEID (YYYYMMDD) 
creditDF = creditDF.withColumn(
    "TIMEID",
        concat(
        col("YEAR"),
        lpad(col("MONTH"),2,"0"), #lPAD JUST MAKES SURE THE FORMAT IS TWO DIGITS, THE 0 MAKES SURE THAT IF THERE ARE ANY SINGLE DIGITS IT WILL ADD A 0 TO THE BEGINNING 
        lpad(col("DAY"),2,"0")
    )
     
)
creditDF = creditDF.drop("YEAR", "MONTH", "DAY") # DROPPING THE UNNECASSARY COLUMNS 
creditDF.show()

# Loading the updated credit card table to db 

creditDF.select('CREDIT_CARD_NO', 'TIMEID', 'CUST_SSN', 'BRANCH_CODE', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'TRANSACTION_ID').write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.Credit") \
  .option("user", "root") \
  .option("password", "password") \
.option("header","false")\
.save()


# ------------ Transforming Customer DataFrame ------------

customerDF.createOrReplaceTempView("Customer")


# convert the first and last name to title case
customerDF = customerDF.withColumn("FIRST_NAME", initcap(col("FIRST_NAME"))).withColumn("LAST_NAME", initcap(col("LAST_NAME")))
customerDF.show()


# converting middle name to lower case 
customerDF = customerDF.withColumn("MIDDLE_NAME", lower(col("MIDDLE_NAME")))
customerDF.show()


# Concat with seperate "," (concatws)
customerDF = customerDF.withColumn(
    "FULL_STREET_ADDRESS",
        concat_ws(
            ",",
            col("STREET_NAME"),
            col("APT_NO")
    )    
)
customerDF = customerDF.drop("STREET_NAME","APT_NO") # DROPPING THE UNNECASSARY COLUMN 
customerDF.show()


# phone number format (xxx)xxx-xxxx

customerDF = customerDF.withColumn( # Using the withColumn to replace the current column with the updated format
    "CUST_PHONE", # The coulmn name (1st arg)
    format_string("%s-%s", # (2nd arg) formats the data. I'm specifiy how I want the data to be formatted with %s place holders for the string value
                  substring("CUST_PHONE",1,3), # Extracts from the CUST_PHONE column. Starts at the 1st character and retrieves 3 chars (the len)
                  substring("CUST_PHONE",4,4)))
customerDF.show()


# loading updated customer table 

customerDF.select('SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CREDIT_CARD_NO', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED').write.format("jdbc") \
  .mode("append") \
  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
  .option("dbtable", "creditcard_capstone.customer") \
  .option("user", "root") \
  .option("password", "password") \
.option("header","false")\
.save()


