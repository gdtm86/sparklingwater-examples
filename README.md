sparklingwater-examples
=======================

This repository that consists of examples of machine learning techinques like bootstrap and clustered bootstrap implemented using Apache Spark and H2o Sparkling Water

Learn more about [Apache Spark](http://spark.apache.org/docs/latest/). 
Learn more about [Apache Spark MLlib and ML](http://spark.apache.org/docs/latest/mllib-guide.html).
Learn more about [H2o Sparkling Water](http://www.h2o.ai/download/sparkling-water/spark15).
Learn more about h2o sparkling water [developer guide](https://github.com/h2oai/sparkling-water/blob/master/DEVEL.md)

Repository consists of the following folders:

- **spark-examples-scala-shell**
- **spark-examples-python-notebooks**
- **h2o-examples-scala-ide**
- **h2o-examples-flow_ui**
- **data**

## spark-examples-scala-shell

This folder conists of examples of generalized linear models with linear regression and Gradient Boosted Machines with bootstrap and clustered bootstrap examples. Examples also include fitting distributed models on bootstrap examples in parallel in spark. 

In these examples, we demonstrate bootstrap on glm using both Apache Spark MLlib and H2o Sparkling Water. Examples include

* Perform bootstrap on glm in parallel using sparkling-water(CDH 5.5.1): *spark-sparkling_water-glm-bootstrap_cdh551.scala*
* Perform clustered bootstrap on gbm in parallel using sparkling-water(CDH 5.5.1): *spark-sparkling_water-gbm-clustered_bootstrap_cdh551.scala*
* Perform bootstrap on glm in parallel using plain apache spark MLlib (CDH 5.5.1): *spark-mllib-glm-clustered_bootstrap_cdh551.scala*
* Perform bootstrap on glm in parallel using plain apache spark MLlib (CDH 5.7.0): *spark-mllib-glm-clustered_bootstrap_cdh570.scala*



## spark-examples-python-notebooks






Introduction to Bootstrap


Introduction to Clustered Bootstrap