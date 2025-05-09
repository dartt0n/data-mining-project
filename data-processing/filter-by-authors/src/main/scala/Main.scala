import org.apache.spark.sql._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._

object Main {
  implicit class ColumnMethods(private val c: Column) {
    def then(t: Column => Column): Column = t(c)
  }

  def trimPrefix(prefixStr: String)(c: Column) = F
    .when(c.startsWith(prefixStr), c.substr(F.lit(prefixStr.length()), F.length(c)))
    .otherwise(c)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("preprocessing")
      .getOrCreate

    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.yandexcloud.net")

    val selector = spark.read
      .parquet("s3a://dataprocdata/authors-works")
      .select(F.col("work"))

    spark.read
      .parquet("s3a://openalex/openalex")
      .join(selector, F.col("id") === F.col("work"), "left")
      .write
      .mode("overwrite")
      .parquet("s3a://dataprocdata/openalex-rich")
  }
}
