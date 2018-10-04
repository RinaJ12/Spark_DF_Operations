package Spark_Group_Id.Spark_Artifact_id

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import java.io.FileNotFoundException
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import scala.util.Try
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.util.Failure
import scala.util.Success

object DataframeOperations {
  
def createDataFrame(fileName:String,fileFormat:String,sqlContext:SQLContext):Option[DataFrame] ={
  try{
    
    fileFormat match {
            case "csv" => { 
                Some(sqlContext.read.format(fileFormat).option("header","true").option("inferSchema","true").option("mode","dropmalformed").load(fileName))
            }
            case _ => Some(sqlContext.read.format(fileFormat).load(fileName))
                    }   
  }
  catch{   
    case fnf:FileNotFoundException => None
    case e:Exception => None
  }
}

 def addColumnRank()(df:DataFrame):Try[DataFrame]= {
    try
    {
      Success(df.withColumn("rank", rank() over (Window.partitionBy("date").orderBy("productCost"))))
    }
    catch{
      case ex:Exception => Failure(ex)
    }
  }
  
  def addColumnProcess()(df:DataFrame):Option[DataFrame]={
    try{
      Some(df.withColumn("extraColumn", upper(col("name"))))
    }
    catch{
      case e:Exception => None
    }
  }
  
def main(args: Array[String])
  {
       val sConf = new SparkConf().setAppName("CreateDataFrame")
        val sContext = new SparkContext(sConf)
        val sqlContext = new SQLContext(sContext)
        val logger:Logger = LogManager.getLogger(DataframeOperations.getClass)
      try
      {       
        logger.info("creating Dataframe...")
        if(args(0).length()>1 && args(1).length()>1)
        {
            val df:DataFrame = createDataFrame(args(0), args(1), sqlContext).get; 
            
            
            df.transform{rec=>
              addColumnRank()(rec) match {            
                case Success(rec) => rec
                case Failure(rec) => null
            }
            }.transform(addColumnProcess()(_).get) 
            
            //Transform eliminates the order dependent variable assignments and make code testable
            // $"col"+1 $"col1" === $"col2"
            
           /** User Defined Funcition
            * sqlContext.register.udf("funciton_name",(s:String)=>s.regex_replace("\\s+",""))
            * df.select("col1",function_name("col2"))
            * 
            */
            
            /*Column Expressions
             * $"col1"!==$"col2" or $"col1".notEqual($"col2")
             * df("col1") && df("col2") $"col1".and(df("col2"))
             * $"col1"===$"col2"
             * df.select("col1".cast("int"))
             * import org.apache.spark.sql.types.IntegerType
             * df.select("col1".cast(IntegerType))
             * df.select("col1").sort($"col1".desc())
             * df.select("col1").sort(df("col1").desc) 
             * df.where($"col1".isNotNull)
             * df.where($"col1".isNull)
             * df.where($"col1".isin(list:_*))
             * */
           
        }
        else
        {
          logger.error("Not enough arguments!!")
        }
       }
      catch
      {
        case e:Exception => logger.error("Exception caught : "+e)
      }
      
  }
}
