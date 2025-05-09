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
  
    spark.read
      .parquet("s3a://openalex/openalex")
      .select(F.col("authors"), F.col("id") as "work")
      .filter(F.size(F.col("authors")) > 1)
      .withColumn("author", F.explode(F.col("authors")))
      .select(F.col("author.id") as "author", F.col("work"))
      .groupBy("author")
      .agg(F.collect_set(F.col("work")) as "works")
      .filter(F.size(F.col("works")) > 1)
      .withColumn("work", F.explode(F.col("works")))
      .groupBy("work")
      .agg(F.collect_set(F.col("author")) as "authors")
      .write
      .mode("overwrite")
      .parquet("s3a://dataprocdata/authors-works")
  }
}
