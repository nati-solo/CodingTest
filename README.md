# Analytic Queries for the Coding Test

* TopCountries file determines the Top 10 countries with the most number of customers and writes the output to top_countries.csv.
* RevenueByCountry file calculates the revenue distribution by country and writes the output to revenue_by_country.csv.
* AvgPriceVolume file determines the relationship between the average unit price of products and their sales volume, plots bar graph & line chart to see the relationship between quantity and average unit price (for Top 25 products selected by total quantity), shows computed statistics, computes the correlation between the average unit price and sales volume across all products and write the output to AvgPriceVolume.csv. 
* MaxPriceDrop file determines the Top 3 products with the maximum unit price drop in the last month and writes the output to products_max_price_drop.csv.
* Validation file performs data validation, cleaning and error handling procedures for all three csv files.
  
# How to run the PySpark application:

Step 1: Make sure you have Spark installed and configured on your system before proceeding.

Step 2: Save the PySpark application code (in .py format) along with the CSV files in the same directory.

Step 3: Open a terminal and navigate to the directory containing the PySpark application and CSV files.

Step 4: Launch a PySpark shell:
```shell script
pyspark
```

Step 5: Within the PySpark shell, run the application code:
```shell script
from app import main
if __name__ == '__main__':
    main()
```
The main() function should:

*	Initialize a SparkSession
*	Use SparkSession to read each CSV file as a DataFrame
*	Perform any data preprocessing/cleansing
*	Execute analysis and aggregation logic
*	Output results
The application will execute on the CSV files and output the results within the PySpark shell.

Step 6: Run the application on a Spark cluster, submit the code to ‘spark-submit’ instead of running locally:
```shell script
spark-submit app.py
```

Step 7: Configure Spark appropriately to connect to the cluster.
This will run the PySpark application and CSV file analysis on the cluster.

Step 8: Once the application has completed its execution, perform any necessary cleanup tasks.
