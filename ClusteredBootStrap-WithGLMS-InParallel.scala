import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vector

println(sc)
println(sqlContext)

val data_df = sqlContext.read
.format("com.databricks.spark.csv")
.option("header", "true") // Use first line of all files as header
.option("inferSchema", "true") //Automatically infer data types
.load("skewdata-policy-new.csv")

data_df.dtypes
data_df.show()

val policyids = data_df.select("policyid").distinct()
policyids.show()

val num_of_samples:Int = 10
val policyid_sample_fraction:Double = 0.8

//Define a function to create clustered samples
def clusteredSamples(data:DataFrame,policies:DataFrame,policyid_sample_fraction:Double,num_of_samples:Int): List[DataFrame] = {
    
    //Initiate an emtpy sample list
    val samples = new ListBuffer[DataFrame]()
    
    for (i <- List.range(0,num_of_samples)){
    	//Create a sample of the unique policy ids
        val policyids_sample = policies.sample(withReplacement=false, fraction=policyid_sample_fraction)
        
        //Sample the data based on the sampled policyids
        val sample = policyids_sample.join(data,policyids_sample("policyid") === data("policyid"),"inner")
        
        //Add the sample to the samples list
        samples += sample
    }
    //We will return a list of clustered samples
    return samples.toList
}

val sampleList = clusteredSamples(data_df,policyids,0.8,10)

//Define a function to run linear regression algorithm on samples in sequence
def runLinearRegression(samples:List[DataFrame]): List[LinearRegressionModel] = {
    //initiate a result list
    val sampleModels = new ListBuffer[LinearRegressionModel]()
    
    //Create a vector Assembler
    val vectorAssembler = new VectorAssembler().setInputCols(Array("age")).setOutputCol("featuresVector")

    //Create a linear regresson model
    val lr = {new LinearRegression()
    		  .setFeaturesCol("featuresVector")
    		  .setLabelCol("values")
    	      .setPredictionCol("predictedValues")
    	      .setMaxIter(5)
    	      .setElasticNetParam(0.5)
    	      .setSolver("l-bfgs")
    	       }

   //Now loop through the samples and fit the regression model
    for(i <- List.range(0,samples.length)){
    	val sample_df = samples(i)
        val sample_df1 = vectorAssembler.transform(sample_df)
        //Fit the linear Regression model
        val sample_lr = lr.fit(sample_df1)
        //Save the coefficients from the Regression model
        sampleModels += sample_lr
    }
    
    //Return the list of regression models from running glm on each sample set    
    return sampleModels.toList
}

val sampleModelsSeq = runLinearRegression(sampleList)
//Print the model coefficients
for (model <- sampleModelsSeq) println(model.coefficients)

//Define a function to run Linear regression algorithm on samples in parallel
def runLinearRegressionParallel(samples:List[DataFrame]): List[LinearRegressionModel] = {
    
    //Create a vector Assembler
    val vectorAssembler = new VectorAssembler().setInputCols(Array("age")).setOutputCol("featuresVector")

    //Create a linear regresson model
    val lr = {new LinearRegression()
    		  .setFeaturesCol("featuresVector")
    		  .setLabelCol("values")
    	      .setPredictionCol("predictedValues")
    	      .setMaxIter(5)
    	      .setElasticNetParam(0.5)
    	      .setSolver("l-bfgs")
    	       }

   //Now loop through the samples and fit the regression model
   def fitModel(sample_df:DataFrame): LinearRegressionModel = {
   		val sample_df1 = vectorAssembler.transform(sample_df)
   		val sample_lr = lr.fit(sample_df1)
   		//return the regression model
   		return sample_lr
   }
   
   val samplesParallel = samples.par 
   val sampleModels =  samplesParallel.map( sample => fitModel(sample))
   //Return the list of regression models from running glm on each bootstrap sample    
   return sampleModels.toList
}

val sampleModelsParallel = runLinearRegressionParallel(sampleList)
//Print the model coefficients
for (model <- sampleModelsParallel) println(model.coefficients)
