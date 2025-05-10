import org.apache.spark.sql._
import org.apache.spark.sql.{functions => F}

/*
root
 |-- id: string (nullable = true)
 |-- doi: string (nullable = true)
 |-- title: string (nullable = true)
 |-- publication_date: string (nullable = true)
 |-- country_codes: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- authors: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- institutions: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- id: string (nullable = true)
 |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |-- country: string (nullable = true)
 |-- concepts: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- score: double (nullable = true)
 |-- topics: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- domain: string (nullable = true)
 |    |    |-- field: string (nullable = true)
 |-- primary_topic: struct (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- domain: string (nullable = true)
 |    |-- field: string (nullable = true)
 |-- referenced_works: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- related_works: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- keywords: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- citations: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- year: long (nullable = true)
 |    |    |-- count: long (nullable = true)
 */

object Main {
  private def handleException(e: Exception): Unit = {
    println("Error occurred: " + e.getMessage)
    e.printStackTrace()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("data-overview")
      .getOrCreate

    import spark.implicits._

    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    sc.hadoopConfiguration.set("fs.s3a.endpoint", "storage.yandexcloud.net");
    sc.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
    sc.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "true")
    sc.hadoopConfiguration.set("fs.s3a.signing-algorithm", "");

    sc.hadoopConfiguration.set("fs.s3a.committer.magic.enabled", "true")
    sc.hadoopConfiguration.set("fs.s3a.connection.maximum", "50")

    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    val df = spark.read
      .parquet("s3a://dataprocdata/openalex-rich")

    try {
      df.select(F.count("*") as "size")
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/dataset_size")
    } catch {
      case e: Exception => handleException(e)
    }

    try {
      df.select(F.explode($"authors") as "author")
        .select(F.countDistinct($"author.id") as "authors")
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/unique_authors")

    } catch {
      case e: Exception => handleException(e)
    }

    try {
      df.select(F.countDistinct($"id") as "works")
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/unique_works")
    } catch {
      case e: Exception => handleException(e)
    }

    try {
      df.select(F.array_sort(F.transform($"authors", c => c.getField("id"))) as "team")
        .select(F.countDistinct($"team") as "teams")
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/unique_teams")
    } catch {
      case e: Exception => handleException(e)
    }

    try {
      df.select(F.explode($"authors") as "author")
        .select(F.explode($"author.institutions") as "institution")
        .select($"institution.country" as "country")
        .groupBy($"country")
        .agg(F.count("*") as "works")
        .sort($"works")
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/works_by_country")
    } catch {
      case e: Exception => handleException(e)
    }

    try {
      df.select(F.explode($"concepts") as "concept")
        .select($"concept.name" as "concept")
        .groupBy($"concept")
        .agg(F.count("*") as "works")
        .sort($"works")
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/works_by_concept")
    } catch {
      case e: Exception => handleException(e)
    }

    try {
      df.select(
          F.when(
            F.size($"citations") > 0,
            F.element_at($"citations", F.size($"citations")).getField("count")
          ).otherwise(F.lit(0)) as "citation_count"
        )
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/citations_distribution")
    } catch {
      case e: Exception => handleException(e)
    }

    try {
      df.select($"primary_topic_flat" as "topic")
        .groupBy($"topic")
        .agg(F.count("*") as "works")
        .sort($"works")
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/works_by_topic")
    } catch {
      case e: Exception => handleException(e)
    }

    try {
      df
        .filter(F.size($"authors") > 1 && F.size($"authors") <= 10)
        .select($"id" as "work", F.explode($"authors") as "author")
        .groupBy($"author.id" as "author")
        .agg(F.collect_list($"work") as "works")
        .filter(F.size($"works") > 1 && F.size($"works") <= 20)
        .select($"author", F.explode($"works") as "work")
        .write
        .mode("overwrite")
        .parquet("s3a://openalex/rich-sample-1-stats/graph/edges")
    } catch {
      case e: Exception => handleException(e)
    }
  }
}
