
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object FirstTask extends App {


  val CORRUPTED = "corrupted"
  val NOT_CORRUPTED = "not_corrupted"
  val spark = SparkSession.builder.
    master("local").
    appName("assignment part 1").
    getOrCreate()
  import spark.implicits._
  val df = spark.read.
    option("header", true).
    schema(midSchema).
    csv("/Users/adityakumartomer/Downloads/SparkHomeWorkAssesment/emp.txt")


  // println("42".asInstanceOf[Int])
  val midSchema = StructType(
    List(
      StructField("name", StringType, nullable = true),
      StructField("age", StringType, nullable = true),
      StructField("salary", StringType, nullable = true),
      StructField("benefits", StringType, nullable = true),
      StructField("department", StringType, nullable = true)
    )
  )
  
  def removeSpacesAndQuotes(rowData: Seq[String]) : Seq[String] = {

    rowData.map ( s => s.replaceAll("\\s", "").replaceAll("\"", ""))

  }

  def removeSpacesAndQuotes = (s: String)  =>  if(s != null) s.replaceAll("\\s", "").replaceAll("\"", "") else s


  val df2 = df.columns.foldLeft(df)((curr, n) => curr.withColumnRenamed(n, n.replaceAll("\\s", "").replaceAll("\"", "") ))

  def check =  (values: Seq[String])  => {
    // println("2"+values(3)+"2")
    values.find(x => x == null || x.isEmpty) match {
      case Some(x) =>
        CORRUPTED
      case None =>
        try {
          val s: Float =
            values(1).toInt
          values(2).toFloat
          values(3).toInt
          NOT_CORRUPTED
        } catch {
          case ex: Exception =>
            println("ex" +ex)
            CORRUPTED
        }


    }
  }

  val udf1 = udf(check)
  val udf2 = udf(removeSpacesAndQuotes)
  val newdf = df2.columns.foldLeft(df2)((curr, n) => curr.withColumn(n, udf2(col(n))))
    .withColumn("status", udf1(array("name", "age", "salary", "benefits", "department")) )


  val filteredDf =  newdf.filter( r => r.getAs[String](5).equals(NOT_CORRUPTED)).withColumn("age", col("age").cast(IntegerType))
    .withColumn("salary", col("salary").cast(FloatType))
    .withColumn("benefits", col("benefits").cast(IntegerType))

  val corruptedDf = newdf.filter( r => r.getAs[String](5).equals(CORRUPTED))

  filteredDf.write.option("header", true).csv("/Users/adityakumartomer/Downloads/SparkHomeWorkAssesment/output.csv")
  corruptedDf.write.option("header", true).csv("/Users/adityakumartomer/Downloads/SparkHomeWorkAssesment/quarantine.csv")








}
