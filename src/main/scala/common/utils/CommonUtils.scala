package common.utils

import java.sql.DriverManager

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer

object CommonUtils {

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


  /** 根据报告期获取报告期别
    * @param bgq
    * @return String
    */
  def getBgqb(bgq:String)= {
    if(bgq.length == 8){
      bgq match {
        case bgq if (bgq(6) != '0' || bgq(7) != '0') => "Y" // 月报
        case bgq if (bgq(5) != '0' ) => "J" // 季报
        case bgq if (bgq(4) == '1' ) => "BN" // 半年报
        case bgq if (bgq(4) == '0' ) => "N" // 年报
        case _ => "E" // 无报告期
      }
    }else{
      throw new IllegalArgumentException("报告期长度不足8位")
    }
  }
  // 将result转化为<k,v> 型数据
  def fromResultToKV(result:Result):Array[(String, String)]  ={
    val mapBuffer = ArrayBuffer[(String,String)]()
    val cf = result.getFamilyMap("info".getBytes())
    val colIt = cf.navigableKeySet().iterator()
    while (colIt.hasNext) {
      val colName = colIt.next()
      val colMap = cf.floorEntry(colName) // 加入判断 满足条件？！的列进入mapBuffer
      mapBuffer.append((Bytes.toString(colMap.getKey), Bytes.toString(colMap.getValue))) // <k, v>   --->   <column, value>
    }
    mapBuffer.toArray
  }


  def hiveJbdcFunction(sql:String) ={
    val hiveDriverName = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(hiveDriverName)
    val hiveConnection = DriverManager.getConnection("jdbc:hive2://hadoop01:10000/result") // batch_hive_increment result
    val hiveStatement = hiveConnection.createStatement()

    try{
      val hiveFlag = hiveStatement.execute(sql)
      println(s"hive flag ${hiveFlag} create table end.")
    }catch { case ex:Exception => ex.printStackTrace() }
    finally{
      hiveConnection.close()
    }

  }
  /**
    * rdd 转 dataframe
    * dataframe 转 rdd
    * rdd 写入 hbase
    *
    */


}
