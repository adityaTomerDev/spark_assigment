import org.apache.spark.sql.SparkSession

object SecondTask extends App {

  val spark = SparkSession.builder().
    master("local").
    appName("assignment part 2").
    getOrCreate()




}
