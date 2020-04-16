package common.utils.mysql

import java.sql.{Date, Timestamp}

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  *
  * ProjectName:    commontest
  * Package:        com.mysql.myutils
  * ClassName:      MysqlUtils
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2019/12/27 15:47
  * Version:        1.0
  */
object MysqlUtils {
  val logger:Logger = Logger.getLogger(getClass.getSimpleName)
  /**
    * 将DataFrame 通过c3p0的连接池方法,向mysql写入数据
    * @param tableName 表名
    * @param resultDateFrame DataFrame
    */

  def saveDFtoDBUsePool(tableName:String ,resultDateFrame:DataFrame){
    val colNumbers = resultDateFrame.columns.length
    val sql = getInsertSql(tableName,colNumbers)
    val columnDataTypes = resultDateFrame.schema.fields.map(_.dataType)
    resultDateFrame.foreachPartition(partitionRecords => {
      val conn = MySQLPoolManager .getMysqlManager.getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null,"％",tableName,"％")//通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(record => {
          //注意:setString方法从1开始,record.getString()方法从0开始
          for(i <- 1 to colNumbers){
            val value = record.get(i - 1)
            val dateType = columnDataTypes(i - 1)
            if(value!= null){//如何值不为空,将类型转换为String
              preparedStatement.setString(i,value.toString)
              dateType match {
                case _:ByteType => preparedStatement.setInt(i,record.getAs [Int](i - 1))
                case _:ShortType => preparedStatement.setInt(i,record.getAs [Int](i - 1))
                case _: IntegerType => preparedStatement.setInt(i,record.getAs [Int](i - 1))
                case _:LongType => preparedStatement.setLong(i,record.getAs [Long](i - 1))
                case _:BooleanType => preparedStatement.setBoolean(i,record.getAs [Boolean](i - 1))
                case _ :FloatType => preparedStatement.setFloat(i,record.getAs [Float](i - 1))
                case _:DoubleType => preparedStatement.setDouble(i,record.getAs [Double](i - 1))
                case _:StringType => preparedStatement.setString(i,record.getAs [String](i - 1))
                case _:TimestampType => preparedStatement.setTimestamp(i,record.getAs [Timestamp](i - 1))
                case _:DateType => preparedStatement.setDate(i,record.getAs [Date](i - 1))
                case _ => throw new RuntimeException(s"nonsupport ${dateType} !!!")
              }
            } else {//如果值为空,将值设为对应类型的空值
              metaData.absolute(i)
              preparedStatement.setNull( i,metaData.getInt("DATA_TYPE"))
            }
          }
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        conn.commit()
      } catch {
        case e:Exception => println(s"@@ saveDFtoDBUsePool ${e.getMessage}")
        //做一些log
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }
  /**
    * 拼装插入SQL
    * @param tableName
    * @param colNumbers
    * @return
    */
  def getInsertSql(tableName:String,colNumbers:Int):String = {
    var sqlStr ="insert into "+ tableName +" values("
    for (i <- 1 to colNumbers){
      sqlStr +="?"
      if(i!= colNumbers){
        sqlStr +=","
      }
    }
    sqlStr +=")"
    println(sqlStr)
    sqlStr
  }

  //以元组的方式返回mysql属性信息
  def getMySQLInfo:(String,String,String)= {
    val jdbcURL = PropertyUtils.getFileProperties("mysql-user.properties","mysql.jdbc.url")
    val userName = PropertyUtils.getFileProperties("mysql-user.properties", "mysql.jdbc.username")
    val passWord= PropertyUtils.getFileProperties("mysql-user.properties", "mysql.jdbc.password")
    (jdbcURL,userName,passWord)
  }

  /**
    * 拼装insertOrUpdate SQL语句
    * @param tableName
    * @param cols
    * @param updateColumns
    * @return
    */
  def getInsertOrUpdateSql(tableName:String,cols:Array [String],updateColumns:Array [String]):String = {
    val colNumbers = cols.length
    var sqlStr ="insert into "+ tableName +" values("
    for(i <- 1 to colNumbers){
      sqlStr +="?"
      if(i!= colNumbers){
        sqlStr +=","
      }
    }
    sqlStr += ") ON DUPLICATE KEY UPDATE "

    updateColumns.foreach(str => {
      sqlStr += s"$str = ?,"
    })

    sqlStr.substring(0,sqlStr.length - 1)
  }

  /**
    * 通过insertOrUpdate的方式把DataFrame写入到MySQL中,注意:此方式,必须对表设置主键
    * @param tableName
    * @param resultDateFrame
    * @param updateColumns
    */
  def insertOrUpdateDFtoDBUsePool(tableName:String,resultDateFrame:DataFrame ,updateColumns:Array [String]){
    val colNumbers = resultDateFrame.columns.length
    val sql = getInsertOrUpdateSql(tableName,resultDateFrame.columns,updateColumns)
    val columnDataTypes: Array[DataType] = resultDateFrame.schema.fields.map(_.dataType)
    println(s"## ############ sql = $sql")
    resultDateFrame.foreachPartition(partitionRecords => {
      val conn = MySQLPoolManager.getMysqlManager.getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null,"％",tableName,"％")//通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false )
        partitionRecords.foreach(record => {
          //注意:setString方法从1开始,record.getString()方法从0开始
          for(i <- 1 to colNumbers){
            val value = record.get(i - 1)
            val dateType = columnDataTypes(i - 1)
            if(value!= null){//如何值不为空,将类型转换为String
              preparedStatement.setString(i,value.toString)
              dateType match {
                case _:ByteType => preparedStatement.setInt(i,record.getAs [Int](i - 1))
                case _:ShortType => preparedStatement.setInt(i,record.getAs [Int](i - 1))
                case _:IntegerType => preparedStatement.setInt(i,record.getAs [Int](i - 1))
                case _:LongType => preparedStatement.setLong(i,record.getAs [Long](i - 1))
                case _:BooleanType => preparedStatement.setInt(i,if(record.getAs [Boolean](i - 1))1 else 0)
                case _:FloatType => preparedStatement.setFloat(i,record.getAs [Float](i - 1))
                case _:DoubleType => preparedStatement.setDouble(i,record.getAs [Double](i - 1))
                case _:StringType => preparedStatement.setString(i,record.getAs [String](i - 1))
                case _:TimestampType => preparedStatement.setTimestamp(i,record.getAs [Timestamp](i - 1))
                case _:DateType => preparedStatement.setDate(i,record.getAs [Date](i - 1))
                case _=>throw new RuntimeException(s"nonsupport '${dateType}' !!!")
              }
            }else{//如果值为空,将值设为对应类型的空值
              metaData.absolute(i)
              preparedStatement.setNull(i, metaData.getInt("DATA_TYPE"))
            }

          }
          //设置需要更新的字段值
          for (ⅰ<- 1 to updateColumns.length){
            val fieldIndex= record.fieldIndex(updateColumns(ⅰ-1))
            val value = record.get(fieldIndex)
            val dataType = columnDataTypes(fieldIndex)
            println(s"@@ $fieldIndex,$value,$dataType")
            if(value!= null){//如何值不为空,将类型转换为String
              dataType match {
                case _:ByteType => preparedStatement.setInt (colNumbers + ⅰ,record.getAs [Int](fieldIndex))
                case _:ShortType => preparedStatement.setInt(colNumbers + ⅰ,record.getAs [Int](fieldIndex))
                case _:IntegerType => preparedStatement.setInt(colNumbers + ⅰ,record.getAs [Int](fieldIndex))
                case _:LongType => preparedStatement.setLong(colNumbers + ⅰ,record.getAs [Long](fieldIndex))
                case _ :BooleanType => preparedStatement.setBoolean(colNumbers + ⅰ,record.getAs [Boolean](fieldIndex))
                case _:FloatType => preparedStatement.setFloat(colNumbers + ⅰ,record.getAs [Float](fieldIndex))
                case _:DoubleType => preparedStatement.setDouble(colNumbers + ⅰ,record.getAs [Double](fieldIndex))
                case _:StringType => preparedStatement.setString(colNumbers + ⅰ,record.getAs [String](fieldIndex))
                case _:TimestampType => preparedStatement.setTimestamp(colNumbers + ⅰ,record.getAs [Timestamp](fieldIndex))
                case _:DateType => preparedStatement.setDate(colNumbers + ⅰ,record.getAs [Date](fieldIndex))
                case _ =>throw new RuntimeException(s"nonsupport ${dataType} !!!")
              }
            } else {//如果值为空,将值设为对应类型的空值
              metaData.absolute(colNumbers + ⅰ)
              preparedStatement.setNull( colNumbers + ⅰ,metaData.getInt("DATA_TYPE"))
            }
          }
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        conn.commit()
      } catch {
        case e:Exception => println(s"@@ insertOrUpdateDFtoDBUsePool ${e.getMessage}")
        //做一些log
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }
}
