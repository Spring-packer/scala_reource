package common.utils.mysql

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  *
  * ProjectName:    commontest
  * Package:        com.mysql.myutils
  * ClassName:      MySQLPoolManager
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2019/12/27 15:07
  * Version:        1.0
  */
object MySQLPoolManager {
  var mysqlManager:MysqlPool = _

  def getMysqlManager:MysqlPool ={
    synchronized{
      if (mysqlManager == null){
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
  class MysqlPool extends  Serializable{

    private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
    try {
      cpds.setJdbcUrl(PropertyUtils.getFileProperties("mysqlprop.properties", "mysql.jdbc.url"))
      cpds.setDriverClass(PropertyUtils.getFileProperties("mysqlprop.properties", "mysql.pool.jdbc.driverClass"))
      cpds.setUser(PropertyUtils.getFileProperties("mysqlprop.properties", "mysql.jdbc.username"))
      cpds.setPassword(PropertyUtils.getFileProperties("mysqlprop.properties", "mysql.jdbc.password"))
      cpds.setMinPoolSize(PropertyUtils.getFileProperties("mysqlprop.properties", "mysql.pool.jdbc.minPoolSize").toInt)
      cpds.setMaxPoolSize(PropertyUtils.getFileProperties("mysqlprop.properties", "mysql.pool.jdbc.maxPoolSize").toInt)
      cpds.setAcquireIncrement(PropertyUtils.getFileProperties("mysqlprop.properties", "mysql.pool.jdbc.acquireIncrement").toInt)  //  如果池中数据连接不够时一次增长多少个
      cpds.setMaxStatements(PropertyUtils.getFileProperties("mysqlprop.properties", "mysql.pool.jdbc.maxStatements").toInt) // JDBC的标准参数，用以控制数据源内加载d的PreparedStatements数量
    } catch {
      case e: Exception => e.printStackTrace()
    }

    def getConnection: Connection = {
      try {
        cpds.getConnection()
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          null
      }
    }

    def close(): Unit = {
      try {
        cpds.close()
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
      }
    }

  }

}
