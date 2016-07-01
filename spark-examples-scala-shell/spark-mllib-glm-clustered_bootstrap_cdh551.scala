import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vector

// Load the dataset into a Spark DataFrame using the spark-csv package .
// You can learn more about the package here at https://github.com/databricks/spark-csv
val data_df = (sqlContext.read
              .format("com.databricks.spark.csv")
              .option("header", "true") // Use first line of all files as header
              .option("inferSchema", "true") //Automatically infer data types
              .load("skewdata-policy-new.csv")
              )

val policyids = data_df.select("policyid").distinct() //Create a policies dataframe
val num_of_samples:Int = 10 //Number of bootstrap samples
val policyid_sample_fraction:Double = 0.8 //Ratio of policy_ids to be included in each bootstrap sample

//Define a function to create clustered samples based on policyid
def clusteredSamples(data:DataFrame,policies:DataFrame,policyid_sample_fraction:Double,num_of_samples:Int): List[DataFrame] = {
    val samples = new ListBuffer[DataFrame]()
    for (i <- List.range(0,num_of_samples)){
        val policyids_sample = policies.sample(withReplacement=false, fraction=policyid_sample_fraction)
        val sample = policyids_sample.join(data,policyids_sample("policyid") === data("policyid"),"inner")
        samples += sample }
    return samples.toList
}

//Define a function to run linear regression algorithm on bootstrap samples in sequence
def runLinearRegression(samples:List[DataFrame]): List[LinearRegressionModel] = {
    
    val sampleModels = new ListBuffer[LinearRegressionModel]() 
    val vectorAssembler = new VectorAssembler().setInputCols(Array("age")).setOutputCol("featuresVector") //Create a vector Assembler
    val lr = {new LinearRegression() //Create a linear regresson model
    		  .setFeaturesCol("featuresVector")
    		  .setLabelCol("values")
    	      .setPredictionCol("predictedValues")
    	      .setMaxIter(5)
    	      .setElasticNetParam(0.5)
    	      //.setSolver("l-bfgs") This setting is only available from Spark 1.6
              }
    for(i <- List.range(0,samples.length)){ //Loop through the samples and fit the regression model
    	val sample_df = samples(i)
        val sample_df1 = vectorAssembler.transform(sample_df)
        val sample_lr = lr.fit(sample_df1) //Fit the linear Regression model
        sampleModels += sample_lr }
    return sampleModels.toList   
}

//Define a function to run Linear regression algorithm on samples in parallel
def runLinearRegressionParallel(samples:List[DataFrame]): List[LinearRegressionModel] = {
    val vectorAssembler = new VectorAssembler().setInputCols(Array("age")).setOutputCol("featuresVector") //Create a vector Assembler    
    val lr = {new LinearRegression() //Create a linear regresson model
    		  .setFeaturesCol("featuresVector")
    		  .setLabelCol("values")
    	      .setPredictionCol("predictedValues")
    	      .setMaxIter(5)
    	      .setElasticNetParam(0.5)
    	      //.setSolver("l-bfgs") This setting is only available from Spark 1.6 
              }
   def fitModel(sample_df:DataFrame): LinearRegressionModel = { //Define a function to fit the regression Model on each dataFrame
   		val sample_df1 = vectorAssembler.transform(sample_df)
   		val sample_lr = lr.fit(sample_df1)
   		return sample_lr
   }
   val samplesParallel = samples.par //Create a parallel collection from samples list
   val sampleModels =  samplesParallel.map( sample => fitModel(sample)) // This runs fitModel in parallel 
   return sampleModels.toList
}

//Create Clustered BootStrap Samples
val sampleList = clusteredSamples(data_df,policyids,policyid_sample_fraction,num_of_samples)

//Generate several linear regression models
val sampleModelsSeq = runLinearRegression(sampleList)
val sampleModelsParallel = runLinearRegressionParallel(sampleList)

//Print the model coefficients
for (model <- sampleModelsSeq) println(model.weights) // It will be model.coefficients from Spark 1.6
for (model <- sampleModelsParallel) println(model.weights) // It will be model.coefficients from Spark 1.6
