package sparkPack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source

object Spark_Gauhar_Obj {

	def main(args:Array[String]):Unit={

			println("====================Execution Initiated====================")
			println

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")

			val sc = new SparkContext(conf)
			sc.setLogLevel("Error")

			val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
			import spark.implicits._

			println("====================Read and Print raw json file====================")
			println

			val raw_df = spark.read.format("json").option("multiLine","true")
			.load("file:///C:/data/complexjson/array1_1.json")

			raw_df.show(10,false)
			raw_df.printSchema()

			println
			println("==================== Array Exploded ====================")
			println

			val explode_df = raw_df.withColumn("Students",explode(col("Students")))
			.withColumn("Students_user_components",explode(col("Students.user.components")))

			explode_df.show(10,false)
			explode_df.printSchema()

			println
			println("==================== Flatten ====================")
			println


			val flatten_df = explode_df.selectExpr("Students.user.address.Permanent_address as Students_user_address_Permanent_address",
					"Students.user.address.temporary_address as Students_user_address_temporary_address",
					"Students.user.gender as Students_user_gender",
					"Students.user.name.first as Students_user_name_first",
					"Students.user.name.last as Students_user_name_last",
					"Students.user.name.title as Students_user_name_title",
					"address.Permanent_address as address_Permanent_address",
					"address.temporary_address as address_temporary_address",
					"first_name",
					"second_name",
					"Students_user_components")
			flatten_df.show(10,false)
			flatten_df.printSchema()        

			println
			println("==================== Execution Terminated ====================")

	}
}