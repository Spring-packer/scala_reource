package common.utils.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory

/**
  *
  * ProjectName:    gxb_resource
  * Package:        common.utils.hbase
  * ClassName:      HBaseUtils
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2020/1/2 13:57
  * Version:        1.0
  */
object HBaseUtils {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private var connection:Connection = null
  private var conf:Configuration = null

  def initConf(): Unit ={
    if( null == conf) {
      conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum","bigdata")
      conf.set("hbase.zookeeper.property.port","2181")
      LOGGER.info("已得到 HBase conf 完成。")
    }
  }
  def initConn(): Unit ={
    if(null == conf) initConf()
    connection = ConnectionFactory.createConnection(conf)
    LOGGER.info("已得到 HBase connection 完成。")
  }

  def getConf(): Configuration ={
    if(null == conf) initConf()
    conf
  }
  def getConn():Connection={
  if(null == connection) initConn()
    connection
  }

  // 读取hbase数据时 使用这个方法得到conf
  def getReadConf(tableName:String):Configuration={
    val conf = getConf()
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)
    conf
  }

  def getNewJobConf(tableName:String): JobConf ={
    val conf = getConf()
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf
  }

  def createTable(tablename:String, columnFamilys:Array[String]=Array("info")):Boolean={

    if(null==conf) initConf()
    conf.setClass("mapreduce.job.outputformat.class", classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[String]], classOf[org.apache.hadoop.mapreduce.OutputFormat[String,Mutation]])
    val hbaseConn = getConn()
    val  admin = hbaseConn.getAdmin
    var table = TableName.valueOf(tablename)
    if(admin.tableExists(table)){
      LOGGER.info(s"${tablename} 已经存在")
      return  true
    } else{
      val desc = new HTableDescriptor(table)
      for(columnFamily <- columnFamilys){
        val columnDesc = new HColumnDescriptor(columnFamily)
        desc.addFamily(columnDesc)
      }
      admin.createTable(desc)
      LOGGER.info(s"${tablename} 创建成功")
      admin.close()
      false
    }
  }

  def deleteHBaseData(tableName:String, family:String, column:String, key:String): Unit ={
    var table: Table = null
    try {
      val table_name:TableName = TableName.valueOf(tableName)
      if(null == connection) initConn()
      table = connection.getTable(table_name)
      val delete: Delete = new Delete(key.getBytes())
      delete.addColumn(family.getBytes(), column.getBytes())
      table.delete(delete) // 删除一个cell 一个单元格
      LOGGER.info(s"${key} 本条数据标记为删除")
    } finally{
      if(null != table) table.close()
    }
  }

  def closeConnection(): Unit ={
    connection.close()
  }





}
