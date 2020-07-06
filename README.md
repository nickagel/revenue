
# Revenue Spark Job
This simple [Apache Spark](https://spark.apache.org/) job uses [Python](https://www.python.org/download/releases/2.7/), Spark SQL & [Docker](https://www.docker.com/). 

The goal of this project is solve 3 problems
- What is the total revenue from orders for each month
- Determine the TOTAL revenue gained from customers acquired in each month
- What percent of revenue comes from new customers for each month

### Setup
```
make docker-build
```
### Run Job
```
make docker-run-job
```
### Run Tests
```
make docker-pytest
```
### Access Docker Container
```
make docker-run
```

## Results
|year_month|  revenue|new_revenue_percentage|revenue_from_new_customers|
|:--------:|--------:|---------------------:|-------------------------:|
|   2019-11|130641.56|                100.00|                 130641.56|
|   2019-12|284833.26|                 83.56|                 238008.78|
|   2020-01|196019.33|                 78.01|                 152912.43|
|   2020-02|180041.82|                 75.62|                 136155.62|
|   2020-03|190745.63|                 77.29|                 147431.21|
|   2020-04| 91988.59|                 76.55|                  70420.93|
|   2020-12| 16940.68|                 71.36|                  12088.83|
