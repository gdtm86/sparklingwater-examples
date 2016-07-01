package com.cloudera.sa.ml.sparklingwater

import org.apache.spark.sql.{SQLContext,DataFrame}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer
import org.apache.spark.h2o
import org.apache.spark.h2o.H2OContext
import scala.collection.mutable.ListBuffer
import _root_.hex.tree.gbm.GBM //Import GBM packages
import _root_.hex.tree.gbm.GBMModel.GBMParameters
import _root_.hex.tree.gbm.GBMModel

/**
 * Created by gmedasani on 6/30/16.
 */
object GBMBootstrap {

  def main (args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage:GBMBootstrap <input-dir> <output-dir>")
      System.exit(1)
    }

    val inputFile = args(0)
    val outputDirectory = args(1)
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    // Load the dataset into a Spark DataFrame using the spark-csv package .
    // You can learn more about the package here at https://github.com/databricks/spark-csv
    val data_df = {sqlContext.read
                  .format("com.databricks.spark.csv")
                  .option("header", "true") // Use first line of all files as header
                  .option("inferSchema", "true") //Automatically infer data types
                  .load(inputFile)}

    val policyids = data_df.select("policyid").distinct() //Create a policies dataframe
    val num_of_samples:Int = 10
    val policyid_sample_fraction:Double = 1.0

    //Define a function to create clustered samples based on policyid
    def clusteredSamples(data:DataFrame,policies:DataFrame,policyid_sample_fraction:Double,num_of_samples:Int): List[DataFrame] = {
      val samples = new ListBuffer[DataFrame]()
      for (i <- List.range(0,num_of_samples)){
        val policyids_sample = policies.sample(withReplacement=false, fraction=policyid_sample_fraction) //Create a sample of the unique policy ids
        val sample = policyids_sample.join(data,policyids_sample("policyid") === data("policyid"),"inner") //Sample the data based on the sampled policyids
        val sample1 = sample.select("age","values")
        samples += sample1 }
      return samples.toList
    }

    //Define a function to run H20 GBM model on samples in sequence
    def runGBMSeq(samples:List[water.Key[water.fvec.Frame]]): List[GBMModel] = {
      val modelsList = new ListBuffer[GBMModel]()
      val response_variable = "values"
      for(i <- List.range(0,samples.length)){ //Loop through the samples and fit the GBM model
      val sample_hf = samples(i)
        val gbmParams = new GBMParameters() //Create a GBM Model Parameters
        gbmParams._distribution = _root_.hex.Distribution.Family.tweedie
        gbmParams._response_column = response_variable
        gbmParams._ntrees = 50
        gbmParams._train = sample_hf //Set the training set for the model
        val sampleGbmModel = new GBM(gbmParams).trainModel.get //Fit the GBM model
        modelsList += sampleGbmModel }
      return modelsList.toList
    }

    //Define a function to run H20 GBM models in parallel
    def runGBMParallel(samples:List[water.Key[water.fvec.Frame]]): List[GBMModel] = {
      def fitGBMModel(sample_hf:water.Key[water.fvec.Frame]): GBMModel = { //Define a function to fit GBM Model on each h2oFrame
      val gbmParams = new GBMParameters() //Create GBM Model parameters
        gbmParams._distribution = _root_.hex.Distribution.Family.tweedie
        gbmParams._response_column = "values"
        gbmParams._ntrees = 50
        gbmParams._train = sample_hf //Set the training set for the model
        val sampleGbmModel = new GBM(gbmParams).trainModel.get //Fit the GBM Model
        return sampleGbmModel
      }
      val samplesParallel = samples.par //Create a parallel collection from samples list
      val modelsList =  samplesParallel.map( h2f => fitGBMModel(h2f)) //This runs fitGBMModel function in parallel
      return modelsList.toList
    }

    // Create Clustered bootstrap samples
    val sampleList = clusteredSamples(data_df,policyids,policyid_sample_fraction,num_of_samples) //Create a sampleList that consists of bootstrap samples
    val h2oFramesList = sampleList.map(df => h2oContext.asH2OFrame(df)) // Create a list of H2oFrames from the bootstrap Spark DataFrames
    val h20FrameKeys = h2oFramesList.map(h2f => h2f.key) //H2o algorithms expect the data to be of type water.key[water.fvec.Frame]

    //Generate several GBMModels on a given list of H2oFrames
    val sampleH20GBMModelsSeq = runGBMSeq(h20FrameKeys)
    val sampleH20GBMModelsParallel = runGBMParallel(h20FrameKeys)

    //Print the model metrics
    for (model <- sampleH20GBMModelsParallel) println("=====> "+ model._output._training_metrics)
    for (model <- sampleH20GBMModelsSeq) println("=====> "+ model._output._training_metrics)

    //If you are developing on your laptop, we can have this keep running and access the H20Flow UI
    h2oContext.stop(true)

  }
}
