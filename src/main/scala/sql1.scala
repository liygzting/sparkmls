import org.apache.spark.sql.SparkSession

object sql1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("run1")
      .getOrCreate()

    // Read CSV file into table
    val df = spark.read.option("header", true)
      .csv("datas/simple-zipcodes.csv")
    df.printSchema()
    df.show()

  }
}
