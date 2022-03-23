# Streamed Pandas Processing Read mE
To make best, efficient use of the notebook to produce output, I suggest pressing the run all cells button (two arrows next to the code drop down) then navigating to cell 11, 
and calling the consume function with your choosen minute/second window 


The notebook can been split into five chapters

1. Setting up libraries, notebook configuration and importing data
You'll noticed I only installed one library (with exception of the optional IPython.core.display to change the notebook width)
This was to keep our code clean and simple, and also guarantee an easy set-up 

2. Processing data and running data quality checks
I load all csvs using the pandas read csv script, given you kindly provided text files.  
Data can also be scrapped online using beautiful soup, or gathered through API calls with postman or the python request library for example. 
As the data is so small, I combined the csvs with a simple concat command, without changing the axis (0 by default) which simply stacks dataframes vertically

I convered our timestamp column into dtatetime. This makes temporal data easy to manipulate, extract into parts and run operations against. 

I created additional columns to ease our insights and analysis going forwards. I split time into minutes and seconds to help answer part 3 and 4. 
I created site column by simply specifying the csv's label and site views with the number of rows in each of the sites csvs. 
I built an assert statement that stops the cell running if all dates are in the future, and an if else elif to return a warning if some dates are in the future, 
and if they stretch over multiple days and hours. The code you provided was helpfully all of the same hour and day, and as such my analysis was formulated respectfully. 

I renamed columns to match the description you provided and placed the time columns at the begging of the dataframe to allow easy viewing

An if statement returns the number of null values in each of the three provided csvs by column, and in the combined csvs columns, if null values are detected

Finally, datatypes are converted to memory efficient types. A warning fires if data is too large to be fit 

3. Breaking the data up into chunks of ten (Producer)
We order our data by time and loop through to yield data 10 rows strong 

4. Gaining insights from the data (consumer)
Simple group by statements with a bit of wrangling produces the output we are looking for after the Producer function is called 

Four masks filter the dataframe by the maximum/minimum time constraints picked by the user 

5. Additional function to remove outliers
Numeric columns are displayed, any number or combination of these are accepted, and outliers are detected and removed. 
