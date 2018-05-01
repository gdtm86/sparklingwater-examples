Sparkling Water, Bootstrap, Clustered Bootstrap and CrossValidation Examples
=============================================================================

![Sparkling Water](http://www.h2o.ai/assets/images/sparkling-water.png)


This repository that consists of examples of machine learning techinques like bootstrap and clustered bootstrap implemented using Apache Spark and H2o Sparkling Water.

Repository consists of the following sections:

- **spark-examples-scala-shell**
- **spark-examples-python-notebooks**
- **h2o-examples-scala-ide**
- **h2o-examples-flow_ui**
- **data**

## spark-examples-scala-shell

This section consists of scala shell examples of generalized linear models with linear regression and Gradient Boosted Machines with bootstrap and clustered bootstrap examples. Examples also include fitting distributed models on bootstrap examples in parallel in spark. 

In these examples, we demonstrate bootstrap on glm using both Apache Spark MLlib and H2o Sparkling Water. Shell scripts include:

* Perform bootstrap on glm in parallel using sparkling-water(CDH 5.5.1): ***spark-sparkling_water-glm-bootstrap_cdh551.scala***
* Perform clustered bootstrap on gbm in parallel using sparkling-water(CDH 5.5.1): ***spark-sparkling_water-gbm-clustered_bootstrap_cdh551.scala***
* Perform bootstrap on glm in parallel using plain apache spark MLlib (CDH 5.5.1): ***spark-mllib-glm-clustered_bootstrap_cdh551.scala***
* Perform bootstrap on glm in parallel using plain apache spark MLlib (CDH 5.7.0): ***spark-mllib-glm-clustered_bootstrap_cdh570.scala***

More on this section [here](spark-examples-scala-shell/README.md)

## spark-examples-python-notebooks

This section consists of ipyton notebooks running in PySpark shell. Notebooks include:

* Calculate confidence intervals using bootstrap technique with PySpark: ***pyspark-confidence_intervals-bootstrap.ipynb***
* Perform clustered bootstrap on glm using PySpark and MLlib: ***pyspark-mllib-glm-clustered_bootstrap.ipynb***
* Perform distributed cross validation of random forests model using PySpark and ML Pipelines: ***pyspark-mllib-randomforests-crossvalidation.ipynb***
* Perform parallel cross validation of single machine scikit-learn models using PySpark and spark-sklearn package: ***pyspark-sklearn-crossvalidation.ipynb***

More on this section [here](spark-examples-python-notebooks/README.md)

## h2o-examples-scala-ide

This section consists of scala code that demonstrates how to develop spark and sparkling water applications in a Scala IDE. Code in this section was built using the IntelliJ IDE. Eclipse is another popular IDE and users can use any IDE of their preference.

Three main classes in this example project are:

* ***com.cloudera.sa.ml.sparklingwater.GBMBootstrap.scala***
* ***com.cloudera.sa.ml.sparklingwater.GlmBootstrap.scala***
* ***com.cloudera.sa.ml.spark.GlmBootstrap.scala***


Compile the code and build a target jar file. Once the jar file is created, copy the jar file to hadoop cluster and submit a spark job as shown below to run Bootstrap on GBM using H2o algorithms.

```
spark-submit --master yarn-cluster --driver-memory 3g --driver-cores 2 --executor-memory 4g --executor-cores 3 
--num-executors 4 --jars jars/commons-csv-1.1.jar,jars/spark-csv_2.10-1.4.0.jar,./sparkling-water-1.5.14/assembly/build/libs/sparkling-water-assembly-1.5.14-all.jar 
--class com.cloudera.sa.ml.sparklingwater.GBMBootstrap ml-examples_2.10-1.0.jar skewdata-policy-new.csv data/output/2
```

## h2o-examples-flow_ui

This section consists of scala flow code that can be imported. Learn more about [H2o flow](http://www.h2o.ai/product/flow/)

* Flow file to perform clustered bootstrap on glm in parallel using spark (CDH 5.5.1): ***spark-mllib-glm-clustered_bootstrap_cdh551.flow***
* Flow file to perform clustered bootstrap on gbm in parallel using sparkling-water (CDH 5.5.1): ***spark-sparkling_water-gbm-clustered_bootstrap_cdh551.flow***
* Flow file to perform bootstrap on glm in parallel using sparkling-water (CDH 5.5.1): ***spark-sparkling_water-glm-bootstrap_cdh551.flow***

You can load the following flow files in H2o flow UI as described on [Flow Guide](http://h2o-release.s3.amazonaws.com/h2o/rel-turchin/3/docs-website/h2o-docs/index.html#%E2%80%A6%20Using%20Flows-Saving%20Flows-Loading%20Flows)

More on this section [here](h2o-examples-flow_ui/README.md)

## Data

Data section consists of all the synthetic datasets that were used in the above scala and python examples. When you download this

* Day.csv 
* Skewdata-policy-new.csv
* skewdata.csv 
* simdata_20K_120vars.csv.zip (compressed)

## Addtional Info

- Learn more about [Apache Spark](http://spark.apache.org/docs/latest/). 
- Learn more about [Apache Spark MLlib and ML](http://spark.apache.org/docs/latest/mllib-guide.html).
- Learn more about [H2o Sparkling Water](http://www.h2o.ai/download/sparkling-water/spark15).
- Learn more about h2o sparkling water [developer guide](https://github.com/h2oai/sparkling-water/blob/master/DEVEL.md)
- Learn more about [Spark-sklearn](https://github.com/databricks/spark-sklearn)
- Learn more about [Bootstrapping (Statistics)](https://en.wikipedia.org/wiki/Bootstrapping_(statistics))
