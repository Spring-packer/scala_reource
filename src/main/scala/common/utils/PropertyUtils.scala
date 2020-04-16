package common.utils

import java.util.Properties

/**
  * explan: PropertyUtils获取resources / mysql-user.properties文件的配置信息
  */
object PropertyUtils {
  def getFileProperties(fileName:String,propertyKey:String):String = {
    val result = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val prop = new Properties
    prop.load(result)
    prop.getProperty(propertyKey)
  }
}
