package hbase

import java.sql.ResultSet

import common.utils.MySQLPoolManager
import common.utils.mysql.MysqlUtils
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  *
  * ProjectName:    gxb_resource
  * Package:        hbase
  * ClassName:      UpdateHBaseFullConfirm
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2020/4/2 9:51
  * Version:        1.0
  *
  * 1. 修改 全量确权表和hive的元数据 4张表增加 business——entity——id 操作 。。。 table表和column表的  更新和新增 两种模式的写入 和
  * 2. 修改hive-hbase的Hbase表名字， 以及受影响的后续逻辑 的修改
  *
  */
object UpdateHBaseFullConfirm {
  val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    //更新列数 行数， 更新columns 表的 新增部分的 列          完成。。。

    val (lines, columns) = (12,Array[String]("var01","var02","var03"))

    // 因为这个表已经存在了 直接更新就行了 其他字段 就没必要了吧
    val sql_update_hbase_full =
      s"""
        |update t_test_hbase_table
        |set col_num = '${lines}' , row_num='${columns.size}'
        |where table_name='BGQ_ORGCODE_TIMEFRAME_TABLE1'
      """.stripMargin
//     update(sql_update_hbase_full)   //更新Table表



    val sql_getOldColumns =
      """
        |select column_name from t_test_hbase_column
        |where  table_name= 'BGQ_ORGCODE_TIMEFRAME_TABLE1'
      """.stripMargin
    val columns_old: Array[String] = getColumns(sql_getOldColumns, "column_name")

    if (!columns_old.isEmpty){
      val column_new = columns.diff(columns_old.distinct)
      column_new.foreach(println)
      //      val sql_columns =
//        """
//          |insert into ... 新增的逻辑
//        """.stripMargin

    }







  }

  def getColumns(sql: String, columnName: String): Array[String] = {
    val conn = MySQLPoolManager.getMysqlManager.getConnection
    val stat = conn.createStatement()
    val arr = ArrayBuffer[String]()
    try {
      val res: ResultSet = stat.executeQuery(sql)
      while (res.next()) {
        arr.append(res.getString(columnName))
      }
      arr.toArray
    } catch {
      case e: Exception => {
        e.printStackTrace()
        arr.clear()
        arr.toArray
      }
    } finally {
      stat.close()
      conn.close()
    }

  }
def update(sql:String): Unit ={
  val conn = MySQLPoolManager.getMysqlManager.getConnection
  val stat = conn.createStatement()
  val arr = ArrayBuffer[String]()
  try {
    val flag: Boolean = stat.execute(sql)
    flag
  } catch {
    case e: Exception => {
      e.printStackTrace()
      false
    }
  } finally {
    stat.close()
    conn.close()
  }
}

}
