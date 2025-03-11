# task1_identify_departments_high_satisfaction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def initialize_spark(app_name="Task1_Identify_Departments"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.

    Parameters:
        spark (SparkSession): The SparkSession object.
        file_path (str): Path to the employee_data.csv file.

    Returns:
        DataFrame: Spark DataFrame containing employee data.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
    df = spark.read.csv("/workspaces/spark-structured-api-employee-engagement-analysis-UmamaheshwarE/input/employee_data.csv", header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    """
    Identify departments with more than 50% of employees having a Satisfaction Rating > 4 and Engagement Level 'High'.

    Parameters:
        df (DataFrame): Spark DataFrame containing employee data.

    Returns:
        DataFrame: DataFrame containing departments meeting the criteria with their respective percentages.
    """
    # Step 1: Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'
    high_satisfaction_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == 'High'))
    
    # Step 2: Calculate the total number of employees per department
    total_employees_per_dept = df.groupBy("Department").count().withColumnRenamed("count", "total_count")
    
    # Step 3: Calculate the number of high satisfaction employees per department
    high_satisfaction_count = high_satisfaction_df.groupBy("Department").count().withColumnRenamed("count", "high_satisfaction_count")
    
    # Step 4: Join both DataFrames
    joined_df = total_employees_per_dept.join(high_satisfaction_count, "Department", "left").fillna(0)
    
    # Step 5: Calculate the percentage of high satisfaction employees
    result_df = joined_df.withColumn(
        "high_satisfaction_percentage",
        spark_round((col("high_satisfaction_count") / col("total_count")) * 100, 2)
    )
    
    # Step 6: Filter departments where high satisfaction percentage is greater than 50%
    result_df = result_df.filter(col("high_satisfaction_percentage") > 5)
    
    # Step 7: Select required columns only
    result_df = result_df.select("Department", "high_satisfaction_percentage")
    
    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.

    Parameters:
        result_df (DataFrame): Spark DataFrame containing the result.
        output_path (str): Path to save the output CSV file.

    Returns:
        None
    """
    # Ensure the output writes to a single CSV file
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 1.
    """
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-UmamaheshwarE/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-UmamaheshwarE/Outputs.csv"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 1
    result_df = identify_departments_high_satisfaction(df)
    
    # Write the result to CSV
    write_output(result_df, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
