package spark.accumulate

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


object TestuseDefine {
  val spark = SparkSession.builder().master("local[2]").appName(this.getClass.getSimpleName).getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    val rdd = sc.makeRDD(Array[String]("h1", "h2","h2","h3"),2)
//    sc.longAccumulator()
    val setWord = new wordAccumulate()
    sc.register(setWord)

    rdd.foreach(x => {
      setWord.add(x)
    })

    println(setWord.value)
println((1, (1,(1,1))))




  }
}
//class wordAccumulate extends
class wordAccumulate extends AccumulatorV2[String, mutable.Set[String]] {
//  private var a =  new util.ArrayList[String]()
  private var setList = mutable.Set[String]()
  override def isZero: Boolean = setList.isEmpty // 是否是初始化状态  这个对象是否是初始化过得

  override def copy(): AccumulatorV2[String, mutable.Set[String]]={
    var newAcc = new wordAccumulate()
    newAcc.setList = this.setList
    newAcc
  }
  override def reset(): Unit = setList.clear()
  override def add(v: String): Unit = {
    if (!setList.contains(v)){
      setList.add(v)
    }
  }
  override def merge(other: AccumulatorV2[String, mutable.Set[String]]): Unit = {
    setList ++= other.value
  }
  override def value: mutable.Set[String] = setList

}
