import org.apache.sedona.spark.SedonaContext

object sedona01 {
  def main(args: Array[String]): Unit = {
    val config = SedonaContext.builder()
      .master("local[*]")
      .appName("sedona01")
      .getOrCreate()

    val sedona = SedonaContext.create(config)
    val rawDf = sedona.read.format("csv")
      .option("delemiter", "\t").option("header", "true")
      .load("datas/nybb.tsv")
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show(5)
  }
}
