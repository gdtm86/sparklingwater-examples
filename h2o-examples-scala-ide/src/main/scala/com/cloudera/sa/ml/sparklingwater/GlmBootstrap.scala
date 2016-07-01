package com.cloudera.sa.ml.sparklingwater

import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.h2o
import _root_.hex.glm.GLM //Import GLM packages
import _root_.hex.glm.GLMModel.GLMParameters
import _root_.hex.glm.GLMModel
import _root_.hex.glm.GLMModel.GLMParameters.Solver.L_BFGS

/**
 * Created by gmedasani on 6/30/16.
 */
object GlmBootstrap {

  def main (args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage:GlmBootstrap <input-dir> <output-dir>")
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
    val simDataDF = {sqlContext.read
                    .format("com.databricks.spark.csv")
                    .option("header", "true") // Use first line of all files as header
                    .option("inferSchema", "true") //Automatically infer data types
                    .load(inputFile)
                    .selectExpr("y_tweedie1","ind1","x1","x2","x4","x5","log_ga1","log_ga2","log_ga1*x2 as log_ga1_x2")}

    val numOfSamples:Int = 10      //number of bootstrap samples to create
    val sampleRatio: Double = 1.0  //Size in ratio compared to the original dataframe size

    //Define a function to create bootstrap samples
    def getBootstrapSamples(simDF:DataFrame, numOfSamples:Int, sampleRatio:Double): List[DataFrame] = {
      val simDFSamples = new ListBuffer[DataFrame]()
      for (i <- List.range(0,numOfSamples)){
        val simDFSample = simDF.sample(withReplacement=true, fraction=sampleRatio)
        simDFSamples += simDFSample }
      return simDFSamples.toList
    }

    //Define a function to run H20 GLM model on samples in sequence
    def runGLMSeq(samples:List[water.Key[water.fvec.Frame]]): List[GLMModel] = {
      val glmModelsList = new ListBuffer[GLMModel]()
      for(i <- List.range(0,samples.length)){ //Loop through the samples and fit the GLM model
      val current_hf = samples(i)
        val glmParams = new GLMParameters() //Create a GLM Model Parameters
        glmParams._distribution = _root_.hex.Distribution.Family.tweedie
        glmParams._response_column = "y_tweedie1"
        glmParams._tweedie_power = 1.8
        glmParams._max_iterations = 100
        glmParams._solver =  _root_.hex.glm.GLMModel.GLMParameters.Solver.L_BFGS
        glmParams._train = current_hf //Set the training set for the model
        val sampleGLMModel = new GLM(glmParams).trainModel.get //Fit the GLM model
        glmModelsList += sampleGLMModel }
      return glmModelsList.toList
    }

    //Define a function to run H20 GLM model on samples in parallel
    def runGLMsParallel(samples:List[water.Key[water.fvec.Frame]]): List[GLMModel] = {
      def fitGLMModel(sample_hf:water.Key[water.fvec.Frame]): GLMModel = { //Define a function to fit GLM Model on each h2oFrame
      val glmParams = new GLMParameters() //Create a GLM Model Parameters
        glmParams._distribution = _root_.hex.Distribution.Family.tweedie
        glmParams._response_column = "y_tweedie1"
        glmParams._tweedie_power = 1.8
        glmParams._max_iterations = 100
        glmParams._solver =  _root_.hex.glm.GLMModel.GLMParameters.Solver.L_BFGS
        glmParams._train = sample_hf //Set the training set for the model
        val sampleGLMModel = new GLM(glmParams).trainModel.get //Fit the GLM model
        return  sampleGLMModel
      }
      val samplesParallel = samples.par //Create a parallel collection from samples list
      val modelsList =  samplesParallel.map( h2f => fitGLMModel(h2f)) //This runs fitGLMModel function in parallel
      return modelsList.toList
    }

    //Create bootstrap samples
    val simDataDFSamplesList = getBootstrapSamples(simDataDF,numOfSamples,sampleRatio)
    val simDataH2FList = simDataDFSamplesList.map(df => h2oContext.asH2OFrame(df))//Convert SparkDataFrames -> h20Frames
    val simDataH2FKeysList = simDataH2FList.map(h2f => h2f.key) // H20 algorithms expect the data to be of type water.Key[water.fvec.Frame]

    //Generate several GLMModels on a given list of H2oFrames
    val sampleH20GLMModelsSeq = runGLMSeq(simDataH2FKeysList)
    val sampleH20GLMModelsParallel = runGLMsParallel(simDataH2FKeysList)

    //Print the model metrics
    for (model <- sampleH20GLMModelsSeq) println("==> "+ model._output._training_metrics)
    println("<===========================================>")
    for (model <- sampleH20GLMModelsParallel) println("==> "+ model._output._training_metrics)

    //If you are developing on your laptop, we can have this keep running and access the H20Flow UI
    h2oContext.stop(true)

    }
  }
