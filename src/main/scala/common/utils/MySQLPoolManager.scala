package common.utils

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource


/**
  * Explans: MySQLPoolManager此类封装了数据库连接池的获取
  */
object MySQLPoolManager {
  var mysqlManager: MysqlPool = _

  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }

  class MysqlPool extends Serializable {
    //数据库的连接池
    private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
    try {
      cpds.setJdbcUrl(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.jdbc.url"))
      cpds.setDriverClass(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.driverClass"))
      cpds.setUser(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.jdbc.username"))
      cpds.setPassword(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.jdbc.password"))
      cpds.setMinPoolSize(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.minPoolSize").toInt)
      cpds.setMaxPoolSize(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.maxPoolSize").toInt)
      cpds.setAcquireIncrement(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.acquireIncrement").toInt)
      cpds.setMaxStatements(PropertyUtils.getFileProperties("mysql-user.properties", "mysql.pool.jdbc.maxStatements").toInt)
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
