package com.gwssi.bigdata

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * https://github.com/Spring-packer/scala_reource.git
  *
  * ProjectName:    gxb_resource
  * Package:        com.gwssi.bigdata
  * ClassName:      Func
  * Description:     类作用描述 
  * Author:          作者：龙飞
  * CreateDate:     2020/4/4 17:48
  * Version:        1.0
  *
  * 首先 f1 f3 不等价 ：
  * 1. f1 是 空参函数 在调用它时 可以加上括号 也可以不加括号
  * 2. f3 是 feature 不是function 在结构类型（Structural Type）中，这两者定义是有区别的。
  * 3. f2 是 返回的是一个匿名函数 f1 返回的是 pp（s）的函数
  *
  *
  */
object Func {

  def f1(): String=>Unit = {
    def pp(s: String):Unit = {
      println(s)
    }
    pp
  }

  def f2(): String => Unit ={
    def pp(s: String):Unit = println(s)
    pp _ // 等价于 pp(_)
  }

  def f3: String => Unit ={
    def pp(s: String): Unit = println(s)
    pp
  }

  /**
    * 关于 f1 和 f3 我读了您关于 “有括号方法和无括号方法区别” 的文章。但是文章只是借structural type 说明了两者是不同的，但是并没有说明不同之处在哪儿。
    * 不过从楼主的附录【1】蛮有启发，准备进一步阅读。 我说的 bug 不是指编译器上的 bug，是指把带括号和不带括号认为是不同的函数签名在语言设计上是不是一个好的决定。
    * 我自己的一点想法是，对于非匿名函数，可以认为是一个 functionN 的一个实例对象，对象名后面加括号那就是默认使用了 apply 函数。但是省略括号的情况下，编
    * 译器做了怎样的处理暂时还不清楚。另外，匿名函数和普通函数的区别也不是很清楚。因为我总感觉匿名函数和普通函数是不同的，但是也没有注意到 scala 包里哪部分的代码是针对匿名函数的。
    * @param args
    */
  def main(args: Array[String]): Unit = {


    val asdasd = "asdasd"
    val strings = Array[String]()
    val schema = StructType(
      Array(StructField("a", StringType, true))
    )
    val a: Row = new GenericRowWithSchema(Array("q1","q2"), schema)
    val row = Row("q1","a2")
//    row.schema = StructType(
//      Array(StructField("a", StringType, true))
//    )

    val aq = Array("")
//aq.find()


    def f(s:String)(r:String) = println(s"$s passed, and then $r passed")

    val a1: String => Unit = f("hello") _

    val a2: String => Unit = (r:String) => f("hello")(r)

    a1(" world!")

    // a1 a2 可以理解位等价  "_" 是包装器
//    val b = 4
//    val d1: Int = b _
//    val d2 = (r:Int) => b(r)
//    val c: ()=>Int = b _
//  println(c)
    /**
      * 对于 val b = 4 使用 val c = b _ 也会使得c成为一个匿名函数，函数签名为 ()=>Int
但是具体的机制怎么样还没弄明白。。。
对于函数 f3，是关于 pp 的使用，pp 编译器怎么编译 pp 取决于 pp 出现的位置。pp 首先是一个对象，function1 之类的对象。有些地方会把 pp 当作函数调用，有些地方会把 pp 当作 函数对象。
这两天写代码的一些感想，不严格。。。
      */


  }
}
