package hbase

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  *
  * ProjectName:    gxb_resource
  * Package:        hbase
  * ClassName:      TestCommon
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2020/4/2 10:41
  * Version:        1.0
  */
object TestCommon {
//  val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
//  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    val arr = ArrayBuffer[String]("1")
    arr.clear()
    println(arr.toArray.isEmpty)

  }
}
