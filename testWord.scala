package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
 


object TestWord {

	def main(args: Array[String]) {
		val conf = new SparkConf()
		.setAppName("WordCount")
		.setMaster("local")
		val sc = new SparkContext(conf)
		val test = sc.textFile("hdfs://sldifrdwbhn01.fr.intranet:8020/tmp/BARRY/simple1.txt")
		test.flatMap { line  =>
		line.split(" ") }
		.map { word => 
		(word, 1)
		}
		.reduceByKey(_ +_)
		.saveAsTextFile("hdfs://sldifrdwbhn01.fr.intranet:8020/tmp/BARRY/simple2.count.txt")
	}
}


/*object Wordcount {

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
				.setMaster("local[*]")
				val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		//val hivecontext = new HiveContext(sc)
		//val resultat = sqlContext.sql(
		// val resultat = sqlContext.sql("SELECT * FROM test7")
		//al resultat = hivecontext.sql("select * from test7")
		//sqlContext.sql("LOAD DATA LOCAL INPATH 'hdfs://sldifrdwbhn01.fr.intranet:8020/tmp/BARRY/simple1.txt' INTO TABLE src")
		val rdd = sqlContext.read.json("hdfs://sldifrdwbhn01.fr.intranet:8020/tmp/BARRY/simple1.txt")
		rdd.saveAsTable("eag_fxticks", "src", SaveMode.Append)
		//rdd.write.saveAsTable("test7")
		//  val options = Map("hdfs://sldifrdwbhn01.fr.intranet:8020/tmp/BARRY/simple1.txt" -> hiveTablePath)
		//rdd.write.format("orc").partitionBy("partitiondate").options(options).mode(SaveMode.Append).saveAsTable(hiveTable)

	}
}*/

