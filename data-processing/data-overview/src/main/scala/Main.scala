import org.apache.spark.SparkContext
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
  implicit class ColumnHelper(val sc: StringContext) extends AnyVal {
    def c(args: Any*): Column = F.col(sc.s(args))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("data-overview")
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

    val df = spark.read
      .parquet("s3a://dataprocdata/openalex-rich")

    df.select(F.count("*") as "size")
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/dataset_size")

    df.select(F.explode(F.col("authors")) as "author")
      .select(F.countDistinct(F.col("author.id")) as "authors")
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/unique_authors")

    df.select(F.countDistinct(F.col("id")) as "works")
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/unique_works")

    df.select(F.array_sort(F.transform(F.col("authors"), c => c.getField("id"))) as "team")
      .select(F.countDistinct(F.col("team")) as "teams")
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/unique_teams")

    df.select(F.explode(F.col("country_codes")) as "country")
      .groupBy(F.col("country"))
      .agg(F.count("*") as "works")
      .sort(F.col("works"))
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/works_by_country")

    df.select(F.explode(F.col("concepts")) as "concept")
      .groupBy(F.col("concept"))
      .agg(F.count("*") as "works")
      .sort(F.col("works"))
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/works_by_concept")

    df.select(
        F.col("id"),
        F.when(
          F.size(F.col("citations")) > 0,
          F.element_at(F.col("citations"), F.size(F.col("citations")) - 1).getField("count")
        ).otherwise(F.lit(0)) as "citation_count"
      )
      .groupBy(F.col("id"))
      .agg(F.max(F.col("citation_count")))
      .select(
        F.max(F.col("citation_count")) as "max",
        F.min(F.col("citation_count")) as "min",
        F.mean(F.col("citation_count")) as "avg",
        F.stddev(F.col("citation_count")) as "std",
      )
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/citations")

    df.select(F.col("primary_topic").getField("name") as "topic")
      .groupBy(F.col("topic"))
      .agg(F.count("*") as "works")
      .sort(F.col("works"))
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/works_by_topic")

    val g = df.sample(1000 / df.count())
        .select(F.col("id") as "work", F.explode(F.col("authors")).getField("id") as "author")

    g.select(F.col("work"))
      .distinct()
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/graph/works")

    g.select(F.col("author"))
      .distinct()
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/graph/authors")

    g.select(F.col("work"), F.col("author"))
      .distinct()
      .write.mode("overwrite").parquet("s3a://openalex/rich-sample-1-stats/graph/edges")
  }
}
