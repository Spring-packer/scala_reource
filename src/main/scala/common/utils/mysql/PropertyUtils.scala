package common.utils.mysql

import java.util.Properties

/**
  *
  * ProjectName:    commontest
  * Package:        com
  * ClassName:      PropertyUtils
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2019/12/27 15:00
  * Version:        1.0
  */
object PropertyUtils {

  def getFileProperties(fileName:String, propertyKey:String):String ={
    val result = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val prop = new Properties()
    prop.load(result)
    prop.getProperty(propertyKey)
  }

}
