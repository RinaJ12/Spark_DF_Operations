package Spark_Group_Id.Spark_Artifact_id

import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions.trim

object ImplicitClass {
  implicit class ColumnMonkeyPatch(c:Column)
  {
    def chain(func:(Column=>Column)):Column ={
      func(c)
    }
    
    def chainUDF(funcName:String,cols:Column*):Column={
      callUDF(funcName,c +: cols:_*)
    }
       
  }
  
   def userDefinedFunc1(s:String):String ={      
      s"ZZZ${s}ZZZZ"
    }
    
    def userDefinedFunc2(s:String):String ={
      s"calling udf on ${s}"
  }
  
  
 def main(args: Array[String]): Unit = {
   
   val sConf = new SparkConf().setAppName("ChainingFuncs")
   val sc = new SparkContext(sConf)
   val sqlContext = new SQLContext(sc)
   val logger:Logger = LogManager.getLogger(ImplicitClass.getClass)
   try{
    val fileInput = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema","true").option("header","true").option("mode","DROPMALFORMED").load("path_to_csv")
    //chaining built in functions to make code more readable
    fileInput.withColumn("extraColumn1", col("anyCol").chain(trim).chain(lower))
    
    //chain udf to organize code
    sqlContext.udf.register("userDefinedFunc11",userDefinedFunc1 _)
    sqlContext.udf.register("userDefinedFunc21",userDefinedFunc2 _)
    
    fileInput.withColumn("udfExtraColumn", col("anyCol").chainUDF("userDefinedFunc11").chainUDF("userDefinedFunc21"))
    fileInput.withColumn("udfExtraColumn", col("anyCol").chainUDF("userDefinedFunc21").chain(lower).chain(lower))
   }
   catch
   {
     case e:Exception =>logger.error("Exception caught : "+e)
   }
 }
}

