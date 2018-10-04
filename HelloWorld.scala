
import org.apache.spark.SparkConf
import java.io.FileNotFoundException
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.util.{Try,Success,Failure}
import org.apache.spark.sql.expressions.Window


object HelloWorld {
  
  def createDF(fileName:String,sqlContext:SQLContext):Option[DataFrame]={
    try{
      val fileInput = sqlContext.read.
                               format("com.databricks.spark.csv").
                               option("header","true").
                               option("inferSchema","true").
                               option("mode","DROPMALFORMED").
                               load(fileName)   
                               
     Some(fileInput)
    }
    catch{
      case fnf:FileNotFoundException => {println(s"$fileName not found!")
      None  
      }
      case ex:Exception => { println(s"$ex Exception")
        None
      }
    }
    
   }
  
 
  def main(args:Array[String])
  {
    val sConf = new SparkConf().setAppName("RetailAnalytics")
    val sc = new SparkContext(sConf)
    val sqlContext = new SQLContext(sc)
    
  
    try{
          
    val fileOriginal = createDF(args(0),sqlContext).get
    val sumPerId = fileOriginal.groupBy("id").agg(sum("purchaseamount").alias("totalPurchase"))
    
   sumPerId.show()                              
   println("Hello World")
    }
    catch
    {
      case e:Exception => println(s"$e Exception unknown!!")
    }
    finally
    {
      sc.stop()
    }
  }
}