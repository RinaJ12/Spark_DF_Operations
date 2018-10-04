package Spark_Group_Id.Spark_Artifact_id

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType

object CustomDFCreation {
  
  /*implicit class SqlContextCreation(sqlContext:SQLContext){
    
    private def asRow[U](values:List[U]):List[Row] ={
      
      values.map{rec => rec match {
        case rec:Row => Row(rec)
        case rec:Product => Row(rec.productIterator.toList:_*)
        case defult => Row(_)
      }
      
      }
    }
    
    private def asSchema[T](field:List[T]):List[StructField]={
      
      field map {
        
        case x:StructField => x.asInstanceOf[Row]
        case (fname:String,fType:DataType,flag:Boolean) => StructField(fname,fType,flag)
        
      }
      
    }
   def createDF[U,T](data:List[U],fieldsType:List[T]) :DataFrame = {
   
   sqlContext.createDataFrame(sqlContext.sparkContext.parallelize(asRow(data)),StructType(asSchema(fieldsType)))
   
   }
  }*/
}