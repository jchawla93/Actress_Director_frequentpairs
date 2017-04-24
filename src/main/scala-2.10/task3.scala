import java.io.FileWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiteshchawla on 10/5/16.
  */
object task3 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("task3").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(sparkConf)
    // Load our input data.
    val sql_cn = new SQLContext(sc)
    val actress_rdd = scala.io.Source.fromFile(args(0)).getLines().toList
    val director_rdd = scala.io.Source.fromFile(args(1)).getLines().toList
    var regularEx = """(["'])(?:(?=(\\?))\2.)*?\1""".r
    var count = 0
    var ac_map = scala.collection.mutable.ListBuffer[(String, String)]()
    var dir_map = scala.collection.mutable.ListBuffer[(String, String)]()

    for (ac <- actress_rdd) {
      var x = regularEx.findAllIn(ac).toList
      ac_map += (x(0) -> x(1))
//              ac_map foreach (x => println (x._1 + "-->" + x._2))
    }
    for (dir <- director_rdd) {
      var y = regularEx.findAllIn(dir).toList
      dir_map += (y(0) -> y(1))
    }
    val ac_rdd : RDD[(String,String)] = sc.parallelize(ac_map)
    val dir_rdd : RDD[(String,String)] = sc.parallelize(dir_map)
    val ac_dir_rdd = ac_rdd.join(dir_rdd)
//    println(ac_dir_rdd)
    val ac = ac_rdd.groupByKey().filter(x=>x._2.toList.length >=2).values.countByValue().map(x=>(x._1.mkString("(",",",")"),x._2))
    //actor-actor tuple
    val dir = dir_rdd.groupByKey().filter(x=>x._2.toList.length >=2).values.countByValue().map(x=>(x._1.mkString("(",",",")"),x._2))
//    println(ac)
//    println(dir)
    var fw = new FileWriter("jitesh_chawla_spark")
    //director-director tuple

    val ac_dir = ac_dir_rdd.filter(x=>x._2.productArity ==2).values.countByValue()
//    println(ac_dir)
//    println(ac_dir)
    //.map(x=>(x._1.mkString("(",",",")"),x._2))

    //actor-director tuple
//    val ab = ac_dir.values.flatMap(i=>i.toList).foreach(println)
//
//    for (i <- ab) {
//      val bc = ab.toString().split(",").toList
//      println(bc)
//    }
//val bh = temp.groupByKey().values.
//      .flatMap(i => i.toList).collect().foreach(println)

//    println(ac.values.countByValue())
//    println(ac_dir)
    val union = ac ++ dir
//    println(union)
    val union_final = union ++ ac_dir
//    union_final.toSeq.sorted
    val result = union_final.map(x=>(x._1.toString,x._2)).toSeq.sortBy(x=>x._2).filter(x=>x._2 >=args(2).toInt)
//    println(result)
    for (i <- result) {
      println(i)
      fw.write(i + "\n")
    }
    fw.close()
  }}