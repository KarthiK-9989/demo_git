import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object helloworld {
  def main(args:Array[String]):Unit={

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "myfirstapplication")
    sparkconf.set("spark.master", "local[*]");

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("path", "C:/Users/Karthik Kondpak/Desktop/Spark_Training/Registration.csv")
      .load()
    df.show()


  }

}


