import common.utils.MySQLUtils

object mysqlTEstJDBC {

  def main(args: Array[String]): Unit = {


    """
      | `filmId` int(11) NOT NULL AUTO_INCREMENT,
      |  `filmName` varchar(50) DEFAULT NULL,
      |  `director` varchar(40) DEFAULT NULL,
      |  `price` float DEFAULT NULL,
      |  `performers` varchar(90) DEFAULT NULL,
      |  `bill` varchar(30) DEFAULT NULL,
      |  `tlength` varchar(10) DEFAULT NULL,
      |  `releaseTime` varchar(30) DEFAULT NULL,
      |  `area` varchar(20) DEFAULT NULL,
      |  `status` int(11) DEFAULT NULL,
    """.stripMargin

    val sql = "select filmName from t_film11"
    val array = MySQLUtils.getOneColumns(sql, "filmName")
    array.foreach(println)

  }
}
