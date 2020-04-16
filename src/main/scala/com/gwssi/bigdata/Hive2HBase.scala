package com.gwssi.bigdata

import java.util.{Properties, UUID}

import common.utils.{HBaseUtils, T_HBASE_BATCH_DEMENSION_TABLE, T_HBASE_FULL_DEMENSION_COLUMN, T_HBASE_FULL_DEMENSION_TABLE}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * : service.sparkjob
  * : com.gwssi.gxb_resource.service.sparkjob
  * : Hive2Hbase
  * : 类作用描述  将多种类型的hive数据表分类存放hbase表中。包括创建多种case组合的hbase表、列簇、列名和数据写入
  * : 作者：龙飞
  * : 2019/12/17 13:28
  * : 更新者：龙飞
  * : 2019/12/17 13:28
  * : 更新说明
  * : 1.0
  */
object Hive2HBase{

  // spark://hadoop01:7077 确定端口
  private val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").enableHiveSupport().getOrCreate()
  private val sc = spark.sparkContext
  private val LOGGER = LoggerFactory.getLogger(Hive2HBase.getClass)
  import spark.implicits._  // 隐式转换

  def main(args: Array[String]): Unit = {
//    val mysql_metadata_batch_id_table = "gxb_resource_data_integration.T_BATCH_TABLE"
//    val mysql_metadata_hive_batch_original_table = "gxb_resource_data_integration.T_HIVE_BATCH_ORIGINAL_TABLE"
//
//    val mysql_metadata_batch_id: String = getMysqlMetadataBatchInfo(mysql_metadata_batch_id_table)
//      .select("batch_id")
//      .first()
//      .getAs[String]("batch_id")
//    LOGGER.info(s"本批次ID： $mysql_metadata_batch_id")
//
//    val original_hive_table_name_list: Array[String] = getMysqlMetadataBatchInfo(mysql_metadata_hive_batch_original_table)
//      .filter($"batch_id" === mysql_metadata_batch_id)
//      .coalesce(1)
//      .collect()
//      .map(row => row.getAs[String]("original_table_name").replace('-', '_').replace(".csv", "")) // 将 BGQ-ORGCODE 转化为 BGQ_ORGCODE
//    LOGGER.info(s"批次$mysql_metadata_batch_id 的 original hive tables：\n\t\t ${original_hive_table_name_list.mkString("\n\t\t")}")

    val mysql_metadata_batch_id = "001"
    val original_hive_table_name_list: Array[String] = Array[String]("bgq_orgcode_timeframe__1585181212")
    // 创建HBase表
    spark.sql("use data") // data 本批 hive的存放数据库
    println("++++++++++++++++++++++++++++++++++++++++")
    val case_hiveTable: Array[(String, String)] = for(i <- original_hive_table_name_list) yield (i.substring(0,i.indexOf("__")),i)

    val case_hive_hbaseBatchTable = for(i <- case_hiveTable) yield (i._1, i._2, ("batch_demension__" + i._1 +"__"+ getBgqb(i._2.split("__")(1).substring(0,8))).toUpperCase)
    case_hive_hbaseBatchTable.foreach(x => LOGGER.info(x.toString()))
    case_hive_hbaseBatchTable.map(_._3).foreach(x => HBaseUtils.createTable(x))
    val case_hive_hbaseFullTable = for(i <- case_hiveTable) yield (i._1, i._2,("full_demension__" + i._1 +"__"+ getBgqb(i._2.split("__")(1).substring(0,8))).toUpperCase)
    case_hive_hbaseFullTable.map(_._3).foreach(x => HBaseUtils.createTable(x))


    // 写入HBase表
    runWriteHiveDataToHBase(case_hive_hbaseBatchTable) // spark 分布式执行
    // 读HBase表
//    readHbaseToMysql(case_hive_hbaseBatchTable,mysql_metadata_batch_id)

  }

  /** 公共函数 根据mysql表名 获取 表数据
    * @param mysqlTableName
    * @return DataFrame
    */
  def  getMysqlMetadataBatchInfo(mysqlTableName:String): DataFrame ={
    val mysql_metadata_dataframe = spark.read.format("jdbc")
      .option("url","jdbc:mysql://hadoop01:3306?characterncoding=UTF-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable",mysqlTableName)
      .option("user","root")
      .option("password","root")
      .option("fetchsize",0).load()
    mysql_metadata_dataframe
  }

  /** 根据报告期获取报告期别
    * @param bgq
    * @return String
    */
  def getBgqb(bgq:String): String ={
    val bgqbArr = Array("N", "BN", "J", "Y", "E")
    if(bgq==null)  bgqbArr(4)
    if(8 != bgq.length) throw new IllegalArgumentException("报告期长度不足8位")
    // "20180100"
//    bgq.toCharArray match {
    ////      case Array(_,_,_,_,1,_*) => ""
    ////    }
    if(bgq.endsWith("00")){
      if(bgq.endsWith("000")){
        if(bgq.endsWith("0000")){
          bgqbArr(0)
        }else{
          bgqbArr(1)
        }
      }else{
        bgqbArr(2)
      }
    }else{
      bgqbArr(3)
    }
  }

  /**
    * @param case_hive_hbase hive的tablename
    */
  def runWriteHiveDataToHBase(case_hive_hbase: Array[(String,String,String)]) = {

    for(i <- case_hive_hbase){ // 拿到batch的hbase表就可以将数据写入了full类型的表中
      val df = spark.sql(s"select * from ${i._2}")
      val column = sc.broadcast(df.columns)
      val rdd: RDD[(ImmutableBytesWritable, Put)] = df.rdd.map(records => {
        val row_key = Bytes.toBytes((for (i <- 1 until i._1.split("_").size) yield records.getAs[String](i)).mkString("_")+"_"+records.getAs[String](0))
        val row = new Put(row_key)
        column.value.zip(records.toSeq).foreach(tp => row.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s"${tp._1}_${i._2}"), Bytes.toBytes(if(null != tp._2) tp._2.toString else "")))
        (new ImmutableBytesWritable, row)
      })
      rdd.saveAsHadoopDataset(HBaseUtils.getNewJobConf(i._3))   // 本集群不适用新的api newapi
      rdd.saveAsHadoopDataset(HBaseUtils.getNewJobConf(i._3.replace("BATCH", "FULL")))
      LOGGER.info("----------------------------------------------")

//      runWriteHBaseMetadataToMysql(s"t_batch_demension__${i._1}")
//      runWriteHBaseMetadataToMysql(s"t_full_demension__${i._1}")
    }
  }

  def readHbaseToMysql(case_hive_hbase: Array[(String, String, String)],mysql_metadata_batch_id:String): Unit = {
    val hbasetables: Array[String] = case_hive_hbase.map(_._3).distinct
    // T_HBASE_FULL_DEMENSION_TABLE Hbase全量表
    val hbaseArr: ArrayBuffer[T_HBASE_FULL_DEMENSION_TABLE] = new ArrayBuffer[T_HBASE_FULL_DEMENSION_TABLE]()
    // T_HBASE_BATCH_DEMENSION_TABLE 全量批次表
    val hbase_batch_arr = new ArrayBuffer[T_HBASE_BATCH_DEMENSION_TABLE]()
    // T_HBASE_FULL_DEMENSION_COLUMN 全量列
    val hbase_column_arr = new ArrayBuffer[T_HBASE_FULL_DEMENSION_COLUMN]()
    val hbaseBroad: Broadcast[ArrayBuffer[T_HBASE_FULL_DEMENSION_COLUMN]] = sc.broadcast(hbase_column_arr)
    for (i <- hbasetables) {
      //println(i.toString())  BATCH_DEMENSION__BGQ_ORGCODE_TIMEFRAME__Y
      val broadcast_set = sc.broadcast(mutable.Set[String]())
      val conf = HBaseUtils.getReadConf(i)
      val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      val rddRow: RDD[(String, Result)] = hbaseRdd.map(tp => (Bytes.toString(tp._2.getRow), tp._2)) // transform <k, v>
      val rddHive: RDD[Array[String]] = rddRow.map({ case (key, result) =>
        val cf = result.getFamilyMap("info".getBytes())
        val colIt = cf.navigableKeySet().iterator()
        val mapBuffer = ArrayBuffer[(String, String)]()
        while (colIt.hasNext) {
          val colName = colIt.next()
          val colMap = cf.floorEntry(colName)
          // 加入判断 满足条件？！的列进入mapBuffer
          mapBuffer.append((Bytes.toString(colMap.getKey), Bytes.toString(colMap.getValue))) // <k, v>   --->   <column, value>
        }
        val cols: Array[String] = mapBuffer.toArray.map(_._1)
        broadcast_set.value ++= cols
        cols
      })
      LOGGER.info("hbase confirm row_num :" + rddHive.count())
      // TODO写入 T_HBASE_FULL_DEMENSION_TABLE 全量过程元数据表
      val tableUUID = UUID.randomUUID().toString
      val tableName = i
      val ddi = "gxb_resource"
      val logic = "ZYK_"+i
      val caseIden = i
      val period = "E"
      val name_zh = i
      val col_num: Int = broadcast_set.value.toArray.size
      val row_num: Int = rddHive.count().toInt
      // T_HBASE_FULL_DEMENSION_TABLE Hbase全量表
      hbaseArr+=(T_HBASE_FULL_DEMENSION_TABLE(tableUUID,tableName,ddi,logic,i,period,name_zh,col_num,row_num))
      // T_HBASE_BATCH_DEMENSION_TABLE 全量批次表
      hbase_batch_arr+= (T_HBASE_BATCH_DEMENSION_TABLE(tableUUID,tableName,ddi,logic,i,period,name_zh,col_num,row_num,mysql_metadata_batch_id))
      // TODO 写入 T_HBASE_FULL_DEMENSION_COLUMN 全量过程元数据列表
      val columnNames: Array[String] = broadcast_set.value.toArray
      //for(y<-columnNames){println(i+" : "+y)} VAR100574_BGQ_ORGCODE_TIMEFRAME__20180007_3__1566247072643
      //println("hbase confirm columns:", broadcast_set.value.toArray.size, broadcast_set.value.toArray.mkString(" || "))
      for (columnName <- columnNames) {
        val columnNa = columnName
        val tableId = tableUUID
        val original_column_name: String = columnName.split("_").head
        val comfirm_source_sys = "gxb_resource"
        val comfirm_update_fre = columnName
        //读取 T_HIVE_BATCH_ORIGINAL_COULUMN 的列的中文和英文名字,是否是主键3个字段
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://10.13.4.101:3306/gxb_resource_data_integration"
        val tableName = "(select column_name,column_label,is_case_identifier from T_HIVE_BATCH_ORIGINAL_COULUMN) temp"
        val user = "root"
        val password = "root"
        val hive_original: DataFrame = spark.sqlContext.read
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("dbtable", tableName)
          .option("user", user)
          .option("password", password)
          .load()
        hive_original.rdd.foreach{
          rdd=>
            val hive_str: String = rdd.mkString("_")
            val hive_strings: Array[String] = hive_str.split("_")
            if(hive_str.contains(original_column_name)){
              hbaseBroad.value.append(T_HBASE_FULL_DEMENSION_COLUMN(columnNa,hive_strings(1),hive_strings(2),tableUUID,original_column_name,comfirm_source_sys,comfirm_update_fre))
            }
        }
      }

    }
    // 全量表
    val hbaseTableDF: DataFrame = sc.makeRDD(hbaseArr).toDF()
    // 全量批次表
    val hbase_batchDF = sc.makeRDD(hbase_batch_arr).toDF()
    //写入mysql T_HBASE_FULL_DEMENSION_COLUMN 全量过程元数据列
    val hbaseColumnDF: DataFrame = sc.makeRDD(hbase_column_arr).toDF()

    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "root")
    props.setProperty("driver", "com.mysql.jdbc.Driver")
    hbaseTableDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://10.13.4.101:3306/gxb_resource_data_integration?useUnicode=true&characterEncoding=utf8", "T_HBASE_FULL_DEMENSION_TABLE", props)
    hbase_batchDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://10.13.4.101:3306/gxb_resource_data_integration?useUnicode=true&characterEncoding=utf8","T_HBASE_BATCH_DEMENSION_TABLE",props)
    hbaseColumnDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://10.13.4.101:3306/gxb_resource_data_integration?useUnicode=true&characterEncoding=utf8", "T_HBASE_FULL_DEMENSION_COLUMN", props)

  }







}
