package com.gwssi.bigdata

import java.sql.{DriverManager, ResultSet}
import java.util
import java.util.Date
import scala.collection.JavaConverters._
import common.utils.HBaseUtils
import org.apache.commons.lang.time.FastDateFormat
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}

/**
  *TODO 1. 编程累加器 自定义累加器 获取列名
  *     2. 转换成 数据流的形式 将其他简单的处理逻辑包装成方法，
  *
  *     让逻辑 简洁 ！清晰 ！
  */
//

object HBase2HBase {
  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate() // 创建spark
  val sc = spark.sparkContext
  sc.setLogLevel("INFO")
  import spark.implicits._
  val LOGGER = LoggerFactory.getLogger(HBase2HBase.getClass)

  //获取当前时间
  val date = new Date
  val instance: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val nowTime: String = instance.format(date)
  //创建 mysql 连接
  val url = "jdbc:mysql://bigdata:3306/phone_shop?characterEncoding=UTF-8"
  val user = "root"
  val password = "root"
  val conn = DriverManager.getConnection(url, user, password)
  val statement = conn.createStatement()

  def main(args: Array[String]): Unit = {

// 元数据驱动版本 动态生成 hbase确权表 hive的新增 更新表

//    val mysql_metadata_batch_id_table = "T_BATCH_TABLE"
//    val mysql_metadata_hbase_batch_demension_table = "T_HBASE_BATCH_DEMENSION_TABLE"
//
//    val mysql_metadata_batch_id = getMysqlMetadataBatchInfo(mysql_metadata_batch_id_table)
//      .select("batch_id")
//      .first()
//      .getAs[String]("batch_id")
//    LOGGER.info(s"本批次ID： $mysql_metadata_batch_id")
//
//    val demension_hbase_table_name_list: Array[String] = getMysqlMetadataBatchInfo(mysql_metadata_hbase_batch_demension_table)
//      .filter($"batch_id" === mysql_metadata_batch_id)
//      .coalesce(1)
//      .collect()
//      .map(row => row.getAs[String]("batch_demension_table_name").replace('-', '_').replace(".csv", "")) // 将 BGQ-ORGCODE 转化为 BGQ_ORGCODE
//    LOGGER.info(s"批次$mysql_metadata_batch_id 的 demension hbase tables：\n\t\t ${demension_hbase_table_name_list.mkString("\n\t\t")}")

    val mysql_metadata_batch_id = "001"
    val demension_hbase_table_name_list: Array[String] = Array[String]("BATCH_DEMENSION__BGQ_ORGCODE_TIMEFRAME__Y")

    for (elem <- demension_hbase_table_name_list) {
      //demension_hbase_table_name_list 本批次的 hbase表数组     获取报告期别：period_type
      val index = elem.lastIndexOf("__")
      val pt = elem.substring(index + 2, elem.length)

      val batch_demension_table = elem  // BATCH_DEMENSION__BGQ_ORGCODE_TIMEFRAME_VAR100856__Y // BATCH_DEMENSION__BGQ_ORGCODE_TIMEFRAME__Y
      val full_demension_table = elem.replace("BATCH", "FULL")
      val full_confirm_table = full_demension_table.replace("DEMENSION", "CONFIRM")

      val case_group = batch_demension_table.split("__")(1)
      val insert_table = (batch_demension_table.replace("BATCH_DEMENSION__", "insert__")+ s"__insertmpp${mysql_metadata_batch_id}").toLowerCase
      val update_table = (batch_demension_table.replace("BATCH_DEMENSION__", "update__")+ s"__updatempp${mysql_metadata_batch_id}").toLowerCase

      LOGGER.info(s"操作用到的数据表 $batch_demension_table , $full_demension_table ," +
        s" $full_confirm_table , insert_table $insert_table , update_table $update_table ")


      val batchHBaseDemension: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(HBaseUtils.getReadConf(batch_demension_table), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      val batchRowKey = batchHBaseDemension.map(tp => (Bytes.toString(tp._2.getRow), Bytes.toString(tp._2.getRow))) // 拼成kv型

//      batchHBaseDemension.take(10).map(x => x._2).foreach(x => println(Bytes.toString(x.getRow),Bytes.toString(x.value)))
      val FullHBaseDemension: RDD[(String, Result)] = sc.newAPIHadoopRDD(HBaseUtils.getReadConf(full_demension_table), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(tp => (Bytes.toString(tp._2.getRow), tp._2))
      val batchRowKey_FullResult: RDD[(String, Result)] = FullHBaseDemension.join(batchRowKey).map(x => (x._1, x._2._1)) // 主要步骤
      batchRowKey_FullResult.cache()

      // 确权rdd 将其写入hbase确权表
      val rdd = batchRowKey_FullResult.map({case(key,result) =>
        val mapbuffer = fromResultToKV(result)
        val cols = mapbuffer.toArray.map(_._1)
        val ror: Array[String] = ruleOfRight(cols)
        val row: Put = new Put(key.getBytes())
        for(i <- ror){
          val map: Map[String, String] = mapbuffer.toMap
          row.addColumn(Bytes.toBytes("info"),
            Bytes.toBytes(i.split('_').head), Bytes.toBytes(map.getOrElse(i, "undefined"))
          ) }  // println(key, i.split('_').head, map.getOrElse(i,"undefined"))
        (new ImmutableBytesWritable, row)
      })
      // 写入确权表
      val isExists: Boolean = HBaseUtils.createTable(full_confirm_table)
      HBaseUtils.createTable(insert_table)
      HBaseUtils.createTable(update_table)
      isExists match {
        case false => {
          println("false")
          println("该case组合还没有comfirm表， 生成新的comfirm表 ")
          rdd.saveAsHadoopDataset(HBaseUtils.getNewJobConf(full_confirm_table))  //"FULL_CONFIRM__BGQ_ORGCODE_TIMEFRAME__Y"
          rdd.saveAsHadoopDataset(HBaseUtils.getNewJobConf(insert_table)) // insert
          if(0 != rdd.count()){
            println("写入到hive表里面 ")
            val collect_col = sc.broadcast(mutable.Set[String]())
            val hbaseDataInsert: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(HBaseUtils.getReadConf(insert_table), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
            val rddRow: RDD[(String, Result)] = hbaseDataInsert.map(tp => (Bytes.toString(tp._2.getRow), tp._2)) // transform <k, v>
            val rddHive = rddRow.map({case(key,result)=>
              val mapBuffer = fromResultToKV(result)
              val cols: Array[String] = mapBuffer.toArray.map(_._1)
              collect_col.value ++= cols
              cols
            })
            val rddhive_count = rddHive.count().toInt
            println("hbase confirm insert row_num :", rddhive_count)
            println("hbase confirm insert columns:", collect_col.value.toArray.size, collect_col.value.toArray.mkString(" || "))
            val columns = collect_col.value.toArray
            val columns_str = columns.map(x => x+" string").mkString(",")
            val columns_str2 = columns.map(x => s"info:$x").mkString(",")
            val tableHive =  insert_table // "insert__BGQ_ORGCODE_TIMEFRAME__Y__updatempp"
            val hbaseTable = insert_table
            val sqlc = // 使用 jdbc 写出 外部表
              s"""
                 |create external table if not exists ${tableHive} (
                 |row_key string,
                 |${columns_str}
                 |) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                 |with serdeproperties(
                 |"hbase.columns.mapping"=
                 |":key, ${columns_str2}")
                 |tblproperties("hbase.table.name"="${hbaseTable}")
      """.stripMargin
            println("working ...  create hbase_hive_insert_table_tmp sql is ", sqlc)
            hiveJbdcFunction(sqlc)
            hiveToHive(insert_table)




            println("start mysql meta date ...")
//            writeMysql1(tableHive, case_group, columns, pt, rddhive_count, mysql_metadata_batch_id, full_confirm_table)
            println("end mysql meta date ...")
          }
        }
        case true => {
          println("true")
          // comfirm表和 batch-demension表 关联查询 该更新更新 该新增新增
          val confirm_data = sc.newAPIHadoopRDD(HBaseUtils.getReadConf(full_confirm_table), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
          val confirm_kv: RDD[(String, Result)] = confirm_data.map(tp => (Bytes.toString(tp._2.getRow), tp._2))
          val unSplit_data: RDD[(String, (Result, Option[Result]))] = batchRowKey_FullResult.leftOuterJoin(confirm_kv)
          val insert_data = unSplit_data.filter(tp => tp._2._2.getOrElse() == None).map(x=> {
            val result = x._2._1
            val put = resultToPut(result)
            (new ImmutableBytesWritable, put)
          })
          insert_data.saveAsHadoopDataset(HBaseUtils.getNewJobConf(insert_table))
          println(insert_data.count(), "-------------存在确权表情况下的 新增数量-------------")
          if(0 != insert_data.count()){
            println("写入到insert hive表里面 ")
            val collect_col = sc.broadcast(mutable.Set[String]())
            val hbaseDataInsert: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(HBaseUtils.getReadConf(insert_table), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
            val rddRow: RDD[(String, Result)] = hbaseDataInsert.map(tp => (Bytes.toString(tp._2.getRow), tp._2)) // transform <k, v>
            val rddHive = rddRow.map({case(key,result)=>
              val mapBuffer = fromResultToKV(result)
              val cols: Array[String] = mapBuffer.toArray.map(_._1)
              collect_col.value ++= cols
              cols
            })
            println("hbase confirm insert row_num :", rddHive.count())
            println("hbase confirm insert columns:", collect_col.value.toArray.size, collect_col.value.toArray.mkString(" || "))
            val columns = collect_col.value.toArray
            val columns_str = columns.map(x => x+" string").mkString(",")
            val columns_str2 = columns.map(x => s"info:$x").mkString(",")
            val tableHive: String = insert_table//"insert__BGQ_ORGCODE_TIMEFRAME__Y__updatempp"
            val hbaseTable: String = insert_table
            val sqlc = // 使用 jdbc 写出 外部表
              s"""
                 |create external table if not exists ${tableHive} (
                 |row_key string,
                 |${columns_str}
                 |) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                 |with serdeproperties(
                 |"hbase.columns.mapping"=
                 |":key, ${columns_str2}")
                 |tblproperties("hbase.table.name"="${hbaseTable}")
              """.stripMargin

            println("working ...  create hbase_hive_insert_table_tmp sql is ", sqlc)
            hiveJbdcFunction(sqlc)
            hiveToHive(insert_table)


          }
          val update_data = unSplit_data.filter(tp => tp._2._2.getOrElse() != None).map(x=>{
            val result = x._2._1
            val put = resultToPut(result)
            (new ImmutableBytesWritable, put)
          })

          update_data.saveAsHadoopDataset(HBaseUtils.getNewJobConf(update_table))
          println(update_data.count(), "-----------存在确权表情况下的 更新数量---------------")
          if(0 != update_data.count()){
            println("写入到update hive表里面 ")
            val collect_col = sc.broadcast(mutable.Set[String]())
            val hbaseDataUpdate: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(HBaseUtils.getReadConf(update_table), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
            val rddRow: RDD[(String, Result)] = hbaseDataUpdate.map(tp => (Bytes.toString(tp._2.getRow), tp._2)) // transform <k, v>
            val rddHive = rddRow.map({case(key,result)=>
              val mapBuffer = fromResultToKV(result)
              val cols: Array[String] = mapBuffer.toArray.map(_._1)
              collect_col.value ++= cols
              cols
            })
            val rddhive_count = rddHive.count().toInt
            println("hbase confirm row_num :", rddhive_count)

            println("hbase confirm columns:", collect_col.value.toArray.size, collect_col.value.toArray.mkString(" || "))
            val columns = collect_col.value.toArray
            val columns_str = columns.map(x => x+" string").mkString(",")
            val columns_str2 = columns.map(x => s"info:$x").mkString(",")
            val tableHive = update_table //"update__BGQ_ORGCODE_TIMEFRAME__Y__updatempp"
            val hbaseTable = update_table
            val sqlc = // 使用 jdbc 写出 外部表
              s"""
                 |create external table if not exists ${tableHive} (
                 |row_key string,
                 |${columns_str}
                 |) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                 |with serdeproperties(
                 |"hbase.columns.mapping"=
                 |":key, ${columns_str2}")
                 |tblproperties("hbase.table.name"="${hbaseTable}")
      """.stripMargin
            println("working ...  create hbase_hive_update_table_tmp sql is ", sqlc)
            hiveJbdcFunction(sqlc)
            hiveToHive(update_table)

          }


        }
      }
    }
    println(" end  ")
  }

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
  // 将result转化为<k,v> 型数据


  def getTmp(result:Result): Unit ={
    val a: mutable.Seq[Cell] = result.getColumnCells("info".getBytes(), "var001".getBytes()).asScala
    val c = a.reduceLeft((a,b) => {
      if(a.getTimestamp > b.getTimestamp) a else b
    })
    Bytes.toString(c.getRow)
    c.getValue








  }
  def fromResultToKV(result:Result)  ={
    val mapBuffer = ArrayBuffer[(String,String)]()
    val cf = result.getFamilyMap("info".getBytes())
    val colIt = cf.navigableKeySet().iterator()
    while (colIt.hasNext) {
      val colName = colIt.next()

      val colMap = cf.floorEntry(colName) // 加入判断 满足条件？！的列进入mapBuffer
      mapBuffer.append((Bytes.toString(colMap.getKey), Bytes.toString(colMap.getValue))) // <k, v>   --->   <column, value>
    }
    mapBuffer
  }

  /**
    * @param result hbase Rsult对象
    * @return hbase put对象
    */
  def resultToPut(result:Result) ={
    val put = new Put(result.getRow)
    val cf = result.getFamilyMap("info".getBytes())
    val colIt = cf.navigableKeySet().iterator()
    while (colIt.hasNext){
      val colMap = cf.floorEntry(colIt.next())
      put.addColumn("info".getBytes(), Bytes.toBytes(Bytes.toString(colMap.getKey).split('_').head), colMap.getValue)
    }
    put
  }


  /**根据 hive表名的最后一个时间戳确权出最新的这个是列的值
    * @param cols row key 对应的列名 Array[String]
    * @return  确权之后的列名 Array[String]
    */
  def ruleOfRight(cols:Array[String]) ={
    //    val columnName =cols.map(_.split('_').head).distinct
    val a: Map[String, String] = cols.map(x => (x.split('_').head, x)).groupBy(_._1)  // group 去重key = (x._1, (x._1, x._2))
      .map(
      x => (x._1,
        x._2.map(y => y._2.substring(y._2.lastIndexOf('_')+1, y._2.size)).sorted.reverse.head  //反转数组取第一个 lasted
      )
    ) // 返回 列名对应的最新的时间戳
    val b: immutable.Iterable[String] = a.map({case(col, ts) =>
      val full_col = cols.find({i:String => i.startsWith(col) && i.endsWith(ts)})
      full_col.getOrElse("undefined")
    })
    b.toArray
  }

  def mysqlJdbcFunction(table:String, columns:Array[String], row:Long=0): Unit ={
    val mysqlDriverName = "com.mysql.jdbc.Driver"
    Class.forName(mysqlDriverName)
    val mysqlConnectionUrl = "jdbc:mysql://hadoop01:3306/gxb_resource_data_integration?user=root&password=root"  // gxb_resource_data_integration
    val mysqlConnection = DriverManager.getConnection(mysqlConnectionUrl)
    val mysqlStatement  = mysqlConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    val case_group = table.split("__")(1)

    for(i <- columns if(i != "row_key")){
      val sql =
        s"""
           |INSERT INTO T_BATCH_CONFIRM_COLUMN
           |(column_id, column_name, column_label, is_case_identifier, batch_confirm_table_id)
           | VALUES("${i}", "${i}", "${i}", "${if(case_group.indexOf(i) != -1) "Y" else  "N"}", "${table}")
      """.stripMargin
      println(sql)
      mysqlStatement.addBatch(sql)
    }
    mysqlStatement.executeBatch()

    try{ // T_BATCH_CONFIRM_TABLE
      val prepare = mysqlConnection.prepareStatement(
        """
          |INSERT INTO T_BATCH_CONFIRM_TABLE
          |(batch_confirm_table_id, batch_confirm_table_name, ddiinstance_urn, logic_storage_id, caseIdentifier, period_type, name_zh, col_num, row_num,
          |create_time,opration_type,batch_id)
          |VALUES(?,?,?,?,?,?,?,?,?,now(),?,?)
        """.stripMargin)

      // id insert__bgq_orgcode_timeframe__mpp_01
      // table insert__bgq_orgcode_timeframe__mpp
      // ddiinstance_urn gxb_resource
      // logic_storage_id ZYK_bgq_orgcode_timeframe
      // caseIdentifier bgq_orgcode_timeframe
      // period E
      // name_zh bgq_orgcode_timeframe_CHAIN
      // col_num
      // row_num
      // create_time now()
      // opration_type insert
      // batch id  001

      prepare.setString(1, table)
      prepare.setString(2, table)
      prepare.setString(3, "gxb_resource")
      prepare.setString(4, "ZYK_"+case_group)
      prepare.setString(5, case_group)
      prepare.setString(6, "E") // period
      prepare.setString(7, table)
      prepare.setString(8, columns.size.toString)
      prepare.setString(9, row.toString)
      prepare.setString(10, "I") // opration_type
      prepare.setString(11, "001")
      println(prepare.getParameterMetaData)
      prepare.executeLargeUpdate()

    }catch { case ex:Exception => ex.printStackTrace() }
    finally{mysqlConnection.close()}
    // ZYK


  }

  def hiveJbdcFunction(sql:String) ={
    val hiveDriverName = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(hiveDriverName)
    val hiveConnection = DriverManager.getConnection("jdbc:hive2://bigdata:10000/result") // batch_hive_increment result
    val hiveStatement = hiveConnection.createStatement()

    try{
      val hiveFlag = hiveStatement.execute(sql)
      println(s"hive flag ${hiveFlag} create table end.")
    }catch { case ex:Exception => ex.printStackTrace() }
    finally{
      hiveConnection.close()
    }

  }

  def hiveToHive(table:String): Unit ={
    val df = spark.sql(s"select * from result.${table}")
    val columns = (for(i <- df.schema.fieldNames if(i != "row_key")) yield i).mkString(",") // hive
    df.createOrReplaceTempView("allcolstable")
    val sql_c =
      s"""
         |create table batch_hive_increment.${table} stored as orc as
         |select ${columns} from allcolstable
       """.stripMargin
    spark.sql(sql_c)
  }

  /*
  def writeMetadataCONFIRM_TABLE(): Unit ={
    val url = "jdbc:mysql://10.13.4.101:3306/gxb_resource_data_integration?createDatabaseIfNotExist=true&amp;characterEncoding=UTF-8&amp;useSSL=false"
    val user = "root"
    val password = "root"
    val sql_result_bgq_orgcode_timeframe_full =
      """
        |insert into T_FULL_CONFIRM_TABLE values(
        |"result_bgq_orgcode_timeframe",
        |"result_bgq_orgcode_timeframe",
        |"gxb_resource",
        |"ZYK_result_bgq_orgcode_timeframe",
        |"result_bgq_orgcode_timeframe",
        |"E",
        |"result_bgq_orgcode_timeframe",
        |33,
        |1221,
        |now()
        |)
      """.stripMargin
    val conn= DriverManager.getConnection(url, user, password)
    val statement = conn.createStatement()

    val mysql1=statement.execute(sql_result_bgq_orgcode_timeframe_full)

    val sql_result_bgq_orgcode_timeframe_batch=
      """
        |insert into T_BATCH_CONFIRM_TABLE values(
        |"result_bgq_orgcode_timeframe",
        |"result_bgq_orgcode_timeframe",
        |"gxb_resource",
        |"ZYK_result_bgq_orgcode_timeframe",
        |"result_bgq_orgcode_timeframe",
        |"E",
        |"result_bgq_orgcode_timeframe",
        |33,
        |1221,
        |now(),
        |"F",
        |"result_bgq_orgcode_timeframe"
        |)
      """.stripMargin
    val mysql2=statement.execute(sql_result_bgq_orgcode_timeframe_batch)

  }

*/


  /*
  def writeMysql1(tableHive:String, case_group:String, columns:Array[String], pt:String,
                  rddhive_count:Int, mysql_metadata_batch_id:String, full_confirm_table:String): Unit ={
    //-----------------------------------------------添加 hive 元数据--------------------------------------------
    //定义 table
    val hive_t_batch_confirm_table_id = null //1
    val hive_t_batch_confirm_table_name: String = tableHive //2
    val hive_t_ddiinstance_urn: String = "gxb_resource" //3
    val hive_t_logic_storage_id: String = "ZYK_" + case_group //4
    val hive_t_caseIdentifier: String = case_group //5
    val hive_t_period_type: String = pt //6
    val hive_t_name_zh: String = tableHive //7
    val hive_t_col_num: Int = columns.size //8
    val hive_t_row_num: Int = rddhive_count //9
    val hive_t_create_time: String = nowTime //10
    val hive_t_operation_type: String = "F" //11
    val hive_t_batch_id: String = mysql_metadata_batch_id //12
    //执行 table
    val metaHiveTable =
    s"""
       |insert into T_HIVE_BATCH_CONFIRM_TABLE values(
       |${hive_t_batch_confirm_table_id},
       |"${hive_t_batch_confirm_table_name}",
       |"${hive_t_ddiinstance_urn}",
       |"${hive_t_logic_storage_id}",
       |"${hive_t_caseIdentifier}",
       |"${hive_t_period_type}",
       |"${hive_t_name_zh}",
       |${hive_t_col_num},
       |${hive_t_row_num},
       |"${hive_t_create_time}",
       |"${hive_t_operation_type}",
       |"${hive_t_batch_id}"
       |)
              """.stripMargin
    println(metaHiveTable)

    statement.execute(metaHiveTable)

    //定义 column
    val hive_c_column_id = null
    val hive_c_batch_confirm_table_name: String = tableHive
    var hive_c_is_case_identifier: String = ""

    //执行 column
    for (column <- columns) {
      if (case_group.contains(column)) {
        hive_c_is_case_identifier = "1"
        val metaHiveColumn =
          s"""
             |insert into T_HIVE_BATCH_CONFIRM_COLUMN values(
             |${hive_c_column_id},
             |"${column}",
             |"${column}",
             |"${hive_c_is_case_identifier}",
             |"${hive_c_batch_confirm_table_name}"
             |)
                 """.stripMargin
        statement.execute(metaHiveColumn)
      } else {
        hive_c_is_case_identifier = "0"
        val metaHiveColumn =
          s"""
             |insert into T_HIVE_BATCH_CONFIRM_COLUMN values(
             |${hive_c_column_id},
             |"${column}",
             |"${column}",
             |"${hive_c_is_case_identifier}",
             |"${hive_c_batch_confirm_table_name}"
             |)
                 """.stripMargin
        statement.execute(metaHiveColumn)
      }
    }
    //-----------------------------------------------添加 hbase 元数据-------------------------------------------
    //定义 table
    val full_confirm_table_id = null
    val full_confirm_table_name: String = full_confirm_table
    val ddiinstance_urn: String = "gxb_resource"
    val logic_storage_id: String = "ZYK_" + case_group
    val caseIdentifier: String = case_group
    val period_type: String = pt
    val name_zh: String = full_confirm_table
    val col_num: Int = rddhive_count
    val row_num: Int = columns.size
    val create_time: String = nowTime
    //执行 table
    val metaHBaseTable =
      s"""
         |insert into T_HBASE_FULL_CONFIRM_TABLE values(
         |${full_confirm_table_id},
         |"${full_confirm_table_name}",
         |"${ddiinstance_urn}",
         |"${logic_storage_id}",
         |"${caseIdentifier}",
         |"${period_type}",
         |"${name_zh}",
         |${col_num},
         |${row_num},
         |"${create_time}"
         |)
              """.stripMargin
    statement.execute(metaHBaseTable)

    //定义 column
    val hbase_c_column_id = null
    var hbase_c_is_case_identifier: String = ""
    val hbase_c_full_confirm_table_name: String = full_confirm_table
    //执行 column
    for (column <- columns) {
      if (case_group.contains(column)) {
        hbase_c_is_case_identifier = "1"
        val metaHBaseColumn =
          s"""
             |insert into T_HBASE_FULL_CONFIRM_COLUMN values(
             |${hbase_c_column_id},
             |"${column}",
             |"${column}",
             |"${hbase_c_is_case_identifier}",
             |"${hbase_c_full_confirm_table_name}"
             |)
                 """.stripMargin
        statement.execute(metaHBaseColumn)
      } else {
        hbase_c_is_case_identifier = "0"
        val metaHBaseColumn =
          s"""
             |insert into T_HBASE_FULL_CONFIRM_COLUMN values(
             |${hbase_c_column_id},
             |"${column}",
             |"${column}",
             |"${hbase_c_is_case_identifier}",
             |"${hbase_c_full_confirm_table_name}"
             |)
                 """.stripMargin
        statement.execute(metaHBaseColumn)
      }
    }

  }
*/

}
