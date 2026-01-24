# PySpark HR Employee Data Pipeline

## Project Overview
Built an end-to-end ETL and analytics pipeline using **Apache Spark (PySpark)** to process employee HR data from CSV files.  
The pipeline performs schema enforcement, SQL analytics, aggregations, filtering, joins, and feature engineering to generate workforce insights.

This project demonstrates real-world **Data Engineering skills** including:
- Spark DataFrames
- Spark SQL
- ETL transformations
- Aggregations
- Schema design
- Data cleaning
- Analytical queries

---

## Tech Stack
- Python
- PySpark
- Spark SQL
- IBM Skills Network Lab

---

## Dataset
Employee records containing:
- Employee ID
- Name
- Salary
- Age
- Department

---

## Implemented Tasks

### Data Ingestion
- Load CSV into Spark DataFrame
- Apply explicit schema

### Transformations & Analytics
- SQL queries
- Average salary by department
- Filter employees by conditions
- Add 10% bonus column
- Self joins
- Aggregations (count, avg, sum, max)
- Sorting
- String filtering

---

## Example Insights
- Average salary per department
- Highest salary by age
- Employee distribution by department
- Bonus-adjusted salaries

---

## Run Locally

```bash
pip install pyspark
python src/hr_pipeline.py
