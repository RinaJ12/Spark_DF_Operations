package Spark_Group_Id.Spark_Artifact_id

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object CustomTransformation {

  def main(args: Array[String]): Unit = {
    
    val sConf = new SparkConf().setAppName("SparkApp")
    val sc = new SparkContext(sConf)
    val sqlContext = new SQLContext(sc)
    //val list = sc.parallelize(List("abc","def","pqr")).toDF("names");
    
   // val values = List(1,2,3,4,5)
    //val df:DataFrame = sqlContext.
  }
 
}