#!/usr/bin/env python
# coding: utf-8
# HR Employee Data Pipeline
# Description:
# ETL pipeline using PySpark to process employee CSV data,
# perform transformations, aggregations, and analytics.
# Given your role as a data engineer, you've been requested to leverage Apache Spark components to accomplish the tasks.
# 
# - Task 1: Generate DataFrame from CSV data.
# - Task 2: Define a schema for the data.
# - Task 3: Display schema of DataFrame.
# - Task 4: Create a temporary view.
# - Task 5: Execute an SQL query.
# - Task 6: Calculate Average Salary by Department.
# - Task 7: Filter and Display IT Department Employees.
# - Task 8: Add 10% Bonus to Salaries.
# - Task 9: Find Maximum Salary by Age.
# - Task 10: Self-Join on Employee Data.
# - Task 11: Calculate Average Employee Age.
# - Task 12: Calculate Total Salary by Department.
# - Task 13: Sort Data by Age and Salary.
# - Task 14: Count Employees in Each Department.
# - Task 15: Filter Employees with the letter o in the Name.
# 

# ### Prerequisites 
# 
# 1. For this project, will be using Python and Spark (PySpark). 

# In[1]:


# Installing required packages 

get_ipython().system('pip install pyspark\u202f findspark wget')


# In[3]:


import findspark

findspark.init()


# In[4]:


# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.   

from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession


# In[5]:


# Creating a SparkContext object  

sc = SparkContext.getOrCreate()

# Creating a SparkSession  

spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


# 2. Download the CSV data.  
# 

# In[6]:


# Download the CSV data first into a local `employees.csv` file
import wget
wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/employees.csv")


# ### Tasks
# 

# #### Task 1: Generate a Spark DataFrame from the CSV data
# 
# Read data from the provided CSV file, `employees.csv` and import it into a Spark DataFrame variable named `employees_df`.
# 
#  
# 

# In[15]:


# Read data from the "emp" CSV file and import it into a DataFrame variable named "employees_df"  
employees_df = spark.read.csv('employees.csv', header = True, inferSchema = True)
employees_df.show()


# #### Task 2: Define a schema for the data
# 
# Construct a schema for the input data and then utilize the defined schema to read the CSV file to create a DataFrame named `employees_df`.  
# 

# In[10]:


# Define a Schema for the input data and read the file using the user-defined Schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
# Define the schema structure
employee_schema = StructType([
    StructField('Emp_No', IntegerType(), True),
    StructField('Emp_Name', StringType(), True),
    StructField('Salary', DoubleType(), True),
    StructField('Age', IntegerType(), True),
    StructField('Department', StringType(), True)
])
# Read the CSV file using the defined schema
employees_df = spark.read.csv('employees.csv', header = True, schema = employee_schema)


# #### Task 3: Display schema of DataFrame
# 
# Display the schema of the `employees_df` DataFrame, showing all columns and their respective data types.  
# 

# In[12]:


# Display all columns of the DataFrame, along with their respective data types
employees_df.printSchema()


# #### Task 4: Create a temporary view
# 
# Create a temporary view named `employees` for the `employees_df` DataFrame, enabling Spark SQL queries on the data. 
# 

# In[16]:


# Create a temporary view named "employees" for the DataFrame
employees_df.createOrReplaceTempView('employees')


# #### Task 5: Execute an SQL query
# 
# Compose and execute an SQL query to fetch the records from the `employees` view where the age of employees exceeds 30. Then, display the result of the SQL query, showcasing the filtered records.
# 

# In[17]:


# SQL query to fetch solely the records from the View where the age exceeds 30
age_over_30 = spark.sql("SELECT * FROM employees WHERE  age > 30 ")
# Display the filtered records
age_over_30.show()


# #### Task 6: Calculate Average Salary by Department
# 
# Compose an SQL query to retrieve the average salary of employees grouped by department. Display the result.
# 

# In[20]:


# SQL query to calculate the average salary of employees grouped by department
avg_salary = spark.sql("SELECT Department, AVG(Salary) as avg_s_emp  FROM employees  GROUP BY Department ")
# Display the filtered records
avg_salary.show()


# #### Task 7: Filter and Display IT Department Employees
# 
# Apply a filter on the `employees_df` DataFrame to select records where the department is `'IT'`. Display the filtered DataFrame.
# 

# In[27]:


# Apply a filter to select records where the department is 'IT'
filtered_it_dep = employees_df.filter(employees_df.Department == 'IT')
# Display the filtered records
filtered_it_dep.show()


# In[28]:


filtered_it_dep = employees_df.filter("Department = 'IT'")
filtered_it_dep.show()


# #### Task 8: Add 10% Bonus to Salaries
# 
# Perform a transformation to add a new column named "SalaryAfterBonus" to the DataFrame. Calculate the new salary by adding a 10% bonus to each employee's salary.
# 

# In[37]:


from pyspark.sql.functions import col

# Add a new column "SalaryAfterBonus" with 10% bonus added to the original salary
employees_df2  = employees_df.withColumn("SalaryAfterBonus", col('Salary') * 1.1)
# Display the transformed records
employees_df2.show()


# #### Task 9: Find Maximum Salary by Age
# 
# Group the data by age and calculate the maximum salary for each age group. Display the result.
# 

# In[38]:


from pyspark.sql.functions import max

# Group data by age and calculate the maximum salary for each age group
max_sal = employees_df.groupBy('age').agg(max('Salary').alias('maximum_salary'))

# Display the maximum salary for each age group
max_sal.show()


# #### Task 10: Self-Join on Employee Data
# 
# Join the "employees_df" DataFrame with itself based on the "Emp_No" column. Display the result.
# 

# In[40]:


# Join the DataFrame with itself based on the "Emp_No" column
emp_self_join = employees_df.alias('e1').join(employees_df.alias('e2'), on = 'Emp_No', how = 'inner')
emp_self_join.show()


# #### Task 11: Calculate Average Employee Age
# 
# Calculate the average age of employees using the built-in aggregation function. Display the result.
# 

# In[44]:


# Calculate the average age of employees
from pyspark.sql.functions import avg 
avg_age_emp = employees_df.agg(avg('Age').alias('average_age'))
avg_age_emp.show()


# #### Task 12: Calculate Total Salary by Department
# 
# Calculate the total salary for each department using the built-in aggregation function. Display the result.
# 

# In[46]:


# Calculate the total salary for each department. Hint - User GroupBy and Aggregate functions
from pyspark.sql.functions import sum 
sal_by_dep = employees_df.groupBy('Department').agg(sum('Salary').alias('total_salary'))

sal_by_dep.show()


# #### Task 13: Sort Data by Age and Salary
# 
# Apply a transformation to sort the DataFrame by age in ascending order and then by salary in descending order. Display the sorted DataFrame.
# 

# In[47]:


# Sort the DataFrame by age in ascending order and then by salary in descending order
sorted_df = employees_df.orderBy(['Age','Salary'], ascending = [True, False])

sorted_df.show()


# #### Task 14: Count Employees in Each Department
# 
# Calculate the number of employees in each department. Display the result.
# 

# In[48]:


from pyspark.sql.functions import count

# Calculate the number of employees in each department
emp_dep = employees_df.groupBy('Department').agg(count('Emp_No').alias('num_of_emp'))
emp_dep.show()


# #### Task 15: Filter Employees with the letter o in the Name
# 
# Apply a filter to select records where the employee's name contains the letter `'o'`. Display the filtered DataFrame.
# 

# In[50]:


# Apply a filter to select records where the employee's name contains the letter 'o'
emp_with_o = employees_df.filter(col('Emp_Name').contains('o'))

emp_with_o.show()


# <!--## Change Log -->
# 

# In[ ]:





# In[ ]:




