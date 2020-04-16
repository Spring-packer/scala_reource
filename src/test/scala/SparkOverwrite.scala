import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable

/**
  *
  * ProjectName:    gxb_resource
  * Package:        
  * ClassName:      SparkOverwrite
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2020/1/6 17:28
  * Version:        1.0
  */
object SparkOverwrite {
  val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._


  def main(args: Array[String]): Unit = {

    // 验证 overwrite  mysql没有主键 和拥有主键时的更新  是否 dataframe 和mysql表 必须同构 必须同构！ overwrite是删除原来的数据， 写入dataframe数据到表中

//    val city: DataFrame = Seq(
//      //      ("jingzhou","CHN","jingzhou",1314011),
//      //      ("xiamen","CHN","xiamen",1314012),
////            ("xinjiang","CHN","xinjiang",1314013),
//      (1,"aomen","CHN",1314014),
//      (2,"aomen","CHN",1314014),
//      (3,"Beijing","CNNC",1886668),
//      (4,"Shanghai","CNN",1996668)
//    ).toDF("id","name", "countrycode", "population")
//
//    city.show()
//    city.write.mode(SaveMode.Overwrite).format("jdbc")
//      .option("url", "jdbc:mysql://bigdata:3306/test")
//      .option("dbtable", "city_hive")
//      .option("user", "root")
//      .option("password", "root")
//      .save()
    val data: DataFrame = getDataSet()
    data.show(false)
    val data1: RDD[Row] = data.toDF().rdd.map{ row =>{
      val list_sch = Array("edu", "City")
      val list_sf = list_sch.map(x => StructField(x, StringType))
      val sch = StructType(row.schema.toBuffer ++ list_sf)
      val new_r: mutable.Seq[String] = Row.unapplySeq(row).get.map(_.asInstanceOf[String]).toBuffer
      val new_r2: mutable.Seq[String] = new_r ++ Seq(null, "Beijing")
      val row2: Row = new GenericRowWithSchema(new_r2.toArray, sch)
      row2
    }
    }
//    data1.toDF().show(false)


  }




  def getDataSet(): DataFrame ={
    val dataList: List[(String, String, String)] = List(
      ("0", "male","no"),
      ("0", "female", "no"),
      ("0", "female", "yes"),
      ("0", "male", "yes"),
      ("0", "male", "no"),
      ("0", "female",  "no"))

    val data: DataFrame = dataList.toDF("affairs", "gender", "age")
    return  data
  }


def getDataFrame(): DataFrame ={
  val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
    (0, "male", 37, 10, "no", 3, 18, 7, 4),
    (0, "female", 27, 4, "no", 4, 14, 6, 4),
    (0, "female", 32, 15, "yes", 1, 12, 1, 4),
    (0, "male", 57, 15, "yes", 5, 18, 6, 5),
    (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
    (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
    (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
    (0, "male", 57, 15, "yes", 2, 14, 4, 4),
    (0, "female", 32, 15, "yes", 4, 16, 1, 2),
    (0, "male", 22, 1.5, "no", 4, 14, 4, 5))

  val data = dataList.toDF("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
  data.printSchema()
  data
}





}
