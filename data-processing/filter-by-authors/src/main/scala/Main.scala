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

    sc.hadoopConfiguration.set("fs.s3a.endpoint", "storage.yandexcloud.net");
    sc.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    sc.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    sc.hadoopConfiguration.set("fs.s3a.signing-algorithm", "");
    
    sc.hadoopConfiguration.set("fs.s3a.committer.magic.enabled", "true")
    sc.hadoopConfiguration.set("fs.s3a.connection.maximum", "50")

    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    val sql = new org.apache.spark.sql.SQLContext(sc)

    val selector = sql.read
      .parquet("s3a://dataprocdata/authors-works")
      .select(F.col("work"))

    spark.read
      .parquet("s3a://dataprocdata/preprocessed_sample_v1")
      .join(selector, F.col("id") === F.col("work"), "inner")
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet("s3a://dataprocdata/openalex-rich-1")

    spark.read
      .parquet("s3a://openalex/openalex")
      .sample(fraction=0.05)
      .join(selector, F.col("id") === F.col("work"), "inner")
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet("s3a://dataprocdata/openalex-rich-5")
  }
}
