package hbase

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * ProjectName:    gxb_resource
  * Package:        hbase
  * ClassName:      hbaseHiveTable
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2020/3/30 16:02
  * Version:        1.0
  */
object hbaseHiveTable {
  val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {

      val sql = "select * from result.update__bgq_orgcode_timeframe__y__updatempp001"
      val a: DataFrame = spark.sql(sql)
    }

  }
}
