package Spark_Group_Id.Spark_Artifact_id

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._


object StructTypeCreation {
  def main(args: Array[String]): Unit = {
    val sConf = new SparkConf().setAppName("StructTypeCreation")
    val sc = new SparkContext(sConf)
    val sqlContext = new SQLContext(sc)
    val logger:Logger = LogManager.getLogger(StructTypeCreation.getClass)
    
    try{
      
      val schema:StructType = StructType(List(
              StructField("col1",IntegerType,true),
              StructField("col2",LongType,true),
              StructField("col3",DoubleType,true),
              StructField("col4",StringType,true)          
                )
              )              
      
       val df = sqlContext.createDataFrame(sc.parallelize(List(Row(1,1111111,100.1111,"ABC"),Row(1,1111111,100.1111,"ABC"),Row(1,1111111,100.1111,"ABC"))),schema)
       df.withColumn("extra_column", struct(col("col1") > 1, col("col2") > 10000))
       df.withColumn("teenage", col("col1").between(13, 19))
       df.withColumn("mood", col("col4").isin(List("happy","glad")))
       df.withColumn("what_to_do", when(col("teenage") && col("mood"),"okay"))
       
     }
    catch{
      case e:Exception => logger.error("Exception caught : "+e)
    }
  }
}