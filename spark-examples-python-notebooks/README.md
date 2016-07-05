Spark Examples Python Notebooks
===============================

This section contains ipython notebooks for various types of Machine learning and statistics techniques that can be performed using PySpark and MLlib.

To start the PySpark shell with IPython Notebooks, follow the blog [Making Python on Apache Hadoop Easier with Anaconda and CDH](http://blog.cloudera.com/blog/2016/02/making-python-on-apache-hadoop-easier-with-anaconda-and-cdh/) by Cloudera Data Scientist Juliet Hougland and Continuum Analytics team.

## Starting PySparkling Shell

```
export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark/
export HADOOP_CONF_DIR=/etc/hadoop/conf
export JAVA_HOME=/usr/java/default/
./sparkling-water-1.6.3/bin/pysparkling --num-executors 3 --executor-memory 2g --executor-cores 2 --driver-memory 2g 
--master yarn-client 
```

Learn more about PySparkling in the Python section at following [PySparkling guide](http://www.h2o.ai/download/sparkling-water/spark15).


## Starting PySpark Shell

```
export PYSPARK_DRIVER_PYTHON=/opt/cloudera/parcels/Anaconda/bin/jupyter
export PYSPARK_PYTHON=/opt/cloudera/parcels/Anaconda/bin/python
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='*' 
--NotebookApp.port=8880"
./bin/pyspark --master yarn --deploy-mode client --name SparkSQLTest --driver-memory 4G --executor-memory 4G 
--executor-cores 3 --num-executors 3 --jars commons-csv-1.1.jar,spark-csv_2.10-1.4.0.jar
```

**pyspark-confidence_intervals-bootstrap.ipynb**

This notebook contains PySpark code that uses Spark DataFrames and bootstrap technique to calculate confidence intervals for a set of values.

Learn more about [Confidence Interval](https://en.wikipedia.org/wiki/Confidence_interval).

Learn more about [Bootstrapping (Statistics)](https://en.wikipedia.org/wiki/Bootstrapping_(statistics))

**pyspark-mllib-randomforests-crossvalidation.ipynb**

This notebook contains PySpark and Spark MLLib/ML code to perform a cross validation of MLlib's random forests algorithm.

 Learn more about PySpark's [CrossValidator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=crossvalidator#pyspark.ml.tuning.CrossValidator)

**pyspark-mllib-glm-clustered-bootstrap.ipynb**

This notebook contains PySpark and Spark MLlib/ML code to perform clustered bootstrap on glm (Gaussian - Linear Regression).

Learn more about [Bootstrapping (Statistics)](https://en.wikipedia.org/wiki/Bootstrapping_(statistics))

**pyspark-sklearn-crossvalidation.ipynb**

This notebook contains pyspark and ML code to perform model selection using cross validation on scikit-learn models. This notebook utilizes spark-sklearn package that integrades Spark and scikit-learn. 

Learn more about the package [spark-sklearn](https://github.com/databricks/spark-sklearn)



