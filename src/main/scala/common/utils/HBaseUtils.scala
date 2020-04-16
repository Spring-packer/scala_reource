package common.utils

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
/**
  * : hiveDemo
  * : com.myhbase.utils
  * : HbaseUtils
  * : 类作用描述
  * : 作者：龙飞
  * : 2019/12/3 17:48
  * : 更新者：龙飞
  * : 2019/12/3 17:48
  * : 更新说明
  * : 1.0
  */
object HBaseUtils {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private var connection:Connection = null
  private var conf:Configuration = null

  def initConf(): Unit ={
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "bigdata")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
  }
  def initConn(): Unit ={
    // 初始化 connection
    if(null == conf){
      initConf()
    }
    connection = ConnectionFactory.createConnection(conf)
  }

  def getConf(): Configuration ={
    if(null == conf){
      initConf()
    }
    conf
  }
  def getConn(): Connection ={
    if (null == connection){
      initConn()
    }
    connection
  }

  /**
    * 读hbase表时使用的 conf hbaseconfig
    * @param tableName
    * @return
    */
  def getReadConf(tableName:String): Configuration = {
    val conf = getConf()
//    conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03,hadoop04,hadoop05")
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE,tableName)
    conf
  }

  // 写入 hbase数据时使用
  def getNewJobConf(tableName:String): JobConf = {
    val conf = getConf()
//    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03,hadoop04,hadoop05")
//    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf
  }

  //返回值为 是否存在
  def createTable(tableName: String, columnFamilys: Array[String] = Array("info")): Boolean = {
//    val conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03,hadoop04,hadoop05")
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
    if(null == conf) initConf() // 获取 hbase conf
     // 添加 output属性
    conf.setClass("mapreduce.job.outputformat.class", classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[String]], classOf[org.apache.hadoop.mapreduce.OutputFormat[String, Mutation]])
//    val hbaseConn = ConnectionFactory.createConnection(conf)
    val hbaseConn = getConn()  // 可能有区别
    val admin = hbaseConn.getAdmin
    var table: TableName = TableName.valueOf(tableName)
    if (admin.tableExists(table)) {
      LOGGER.info(s"${tableName} already exists ! ")
      true
    } else {
      val desc: HTableDescriptor = new HTableDescriptor(table)
      for (columnFamily <- columnFamilys) {
        val columnDesc: HColumnDescriptor = new HColumnDescriptor(columnFamily)
        desc.addFamily(columnDesc)
      }
      admin.createTable(desc)
      LOGGER.info(s"created table $tableName ! ")
      admin.close()
      false
    }

  }
  //删除某条记录
  def deleteHbaseData(tableName:String,family:String,column:String,key:String): Unit ={
    var table:Table=null
    try{
      val tablename: TableName = TableName.valueOf(tableName)
      val table: Table = connection.getTable(tablename)
      val delete = new Delete(key.getBytes())
      delete.addColumn(family.getBytes(),column.getBytes())
      table.delete(delete)
      println("delete record done")
    }finally {
      if(table!=null)table.close()
    }
  }
  //根据rowkey 删除一条记录
  def deleteHbaseRowKey(tableName:String,key:String): Unit ={
    var table:Table=null
    if(connection == null) getConn()
    try{
      val tablename: TableName = TableName.valueOf(tableName)
      val table: Table = connection.getTable(tablename)
      val delete = new Delete(key.getBytes())
//      delete.addColumn(family.getBytes(),column.getBytes())
      table.delete(delete)
      println("delete record done")
    }finally {
      if(table!=null)table.close()
    }
  }

  //根据rowkey 删除一条记录
  def deleteHbaseRowKeyList(tableName:String,keys:Array[String]): Unit ={
    var table:Table=null
    if(connection == null) getConn()
    import scala.collection.JavaConversions._
    try{
      val tablename: TableName = TableName.valueOf(tableName)
      val table: Table = connection.getTable(tablename)
      val delete_keys: Array[Delete] = keys.map(x => new Delete(x.getBytes()))
      table.delete(delete_keys.toList)
      println(s"delete ${delete_keys.length} record done")
    }finally {
      if(table!=null)table.close()
    }
  }
  //table.delete(list) 批量删除  (family,column,rkData)
  def deleteHbaseListData(tableName:String,tuples: ArrayBuffer[(String, String, String)]): Unit ={
    var table:Table=null
    try{
      val tablename: TableName = TableName.valueOf(tableName)
      val table = connection.getTable(tablename)
      val deletes: util.ArrayList[Delete] = new util.ArrayList[Delete]()
      for(tuple<-tuples){
        val delete = new Delete(tuple._3.getBytes())
        delete.addColumn(tuple._1.getBytes(),tuple._2.getBytes())
        deletes.add(delete)
      }
      table.delete(deletes)
      println("delete record done")
    }finally {
      if(table!=null)table.close()
    }
  }

  def closeConnection(): Unit = {
    connection.close()
  }

}
