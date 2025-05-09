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

    val SCHEMA = StructType(
      Array(
        StructField(
          "authorships",
          ArrayType(
            StructType(
              Array(
                StructField(
                  "author",
                  StructType(
                    Array(
                      StructField("display_name", StringType, true),
                      StructField("id", StringType, true),
                      StructField("orcid", StringType, true)
                    )
                  ),
                  true
                ),
                StructField("countries", ArrayType(StringType, true), true),
                StructField(
                  "institutions",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("country_code", StringType, true),
                        StructField("display_name", StringType, true),
                        StructField("id", StringType, true)
                      )
                    ),
                    true
                  ),
                  true
                )
              )
            ),
            true
          ),
          true
        ),
        StructField(
          "concepts",
          ArrayType(
            StructType(
              Array(
                StructField("display_name", StringType, true),
                StructField("id", StringType, true),
                StructField("level", LongType, true),
                StructField("score", DoubleType, true),
                StructField("wikidata", StringType, true)
              )
            ),
            true
          ),
          true
        ),
        StructField(
          "counts_by_year",
          ArrayType(
            StructType(
              Array(
                StructField("cited_by_count", LongType, true),
                StructField("year", LongType, true)
              )
            ),
            true
          ),
          true
        ),
        StructField("created_date", StringType, true),
        StructField("display_name", StringType, true),
        StructField("doi", StringType, true),
        StructField("id", StringType, true),
        StructField(
          "keywords",
          ArrayType(
            StructType(
              Array(
                StructField("display_name", StringType, true),
                StructField("id", StringType, true),
                StructField("score", DoubleType, true)
              )
            ),
            true
          ),
          true
        ),
        StructField(
          "primary_topic",
          StructType(
            Array(
              StructField("display_name", StringType, true),
              StructField(
                "domain",
                StructType(
                  Array(
                    StructField("display_name", StringType, true),
                    StructField("id", StringType, true)
                  )
                ),
                true
              ),
              StructField(
                "field",
                StructType(
                  Array(
                    StructField("display_name", StringType, true),
                    StructField("id", StringType, true)
                  )
                ),
                true
              ),
              StructField("id", StringType, true),
              StructField("score", DoubleType, true),
              StructField(
                "subfield",
                StructType(
                  Array(
                    StructField("display_name", StringType, true),
                    StructField("id", StringType, true)
                  )
                ),
                true
              )
            )
          ),
          true
        ),
        StructField("publication_date", StringType, true),
        StructField("referenced_works", ArrayType(StringType, true), true),
        StructField("country_codes", ArrayType(StringType, true), true),
        StructField("related_works", ArrayType(StringType, true), true),
        StructField("title", StringType, true),
        StructField(
          "topics",
          ArrayType(
            StructType(
              Array(
                StructField("display_name", StringType, true),
                StructField(
                  "domain",
                  StructType(
                    Array(
                      StructField("display_name", StringType, true),
                      StructField("id", StringType, true)
                    )
                  ),
                  true
                ),
                StructField(
                  "field",
                  StructType(
                    Array(
                      StructField("display_name", StringType, true),
                      StructField("id", StringType, true)
                    )
                  ),
                  true
                ),
                StructField("id", StringType, true),
                StructField("score", DoubleType, true),
                StructField(
                  "subfield",
                  StructType(
                    Array(
                      StructField("display_name", StringType, true),
                      StructField("id", StringType, true)
                    )
                  ),
                  true
                )
              )
            ),
            true
          ),
          true
        )
      )
    )

    val spark = SparkSession
      .builder()
      .appName("preprocessing")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    spark.read
      .option("compression", "gzip")
      .option("mode", "DROPMALFORMED")
      .option("multiLine", "false")
      .option("inferSchema", "false")
      .schema(SCHEMA)
      .json("s3a://openalex/data/works/**/*.gz")
      .select(
        F.col("id").then(trimPrefix("https://openalex.org/")).alias("id"),
        F.col("doi").then(trimPrefix("https://doi.org/")).alias("doi"),
        F.col("title").alias("title"),
        F.col("publication_date").alias("publication_date"),
        F.col("country_codes").alias("country_codes"),
        F.col("authorships")
          .then(
            F.transform(
              _,
              author =>
                F.struct(
                  author
                    .getField("author")
                    .getField("id")
                    .then(trimPrefix("https://openalex.org/"))
                    .alias("id"),
                  author
                    .getField("author")
                    .getField("display_name")
                    .alias("name"),
                  author
                    .getField("institutions")
                    .then(c =>
                      F.when(
                        c.isNotNull,
                        F.transform(
                          c,
                          institution =>
                            F.struct(
                              institution
                                .getField("id")
                                .then(trimPrefix("https://openalex.org/"))
                                .alias("id"),
                              institution
                                .getField("display_name")
                                .alias("name"),
                              institution
                                .getField("country_code")
                                .alias("country")
                            )
                        )
                      ).otherwise(F.lit(null))
                        .alias("institutions")
                    )
                )
            )
          )
          .alias("authors"),
        F.col("concepts")
          .then(
            F.transform(
              _,
              concept =>
                F.struct(
                  concept
                    .getField("id")
                    .then(trimPrefix("https://openalex.org/"))
                    .alias("id"),
                  concept
                    .getField("display_name")
                    .alias("name"),
                  concept
                    .getField("score")
                    .alias("score")
                )
            )
          )
          .alias("concepts"),
        F.col("topics")
          .then(
            F.transform(
              _,
              topic =>
                F.struct(
                  topic
                    .getField("id")
                    .then(trimPrefix("https://openalex.org/"))
                    .alias("id"),
                  topic
                    .getField("display_name")
                    .alias("name"),
                  topic
                    .getField("domain")
                    .getField("display_name")
                    .alias("domain"),
                  topic
                    .getField("field")
                    .getField("display_name")
                    .alias("field")
                )
            )
          )
          .alias("topics"),
        F.struct(
          F.col("primary_topic").getField("display_name").alias("name"),
          F.col("primary_topic").getField("domain").getField("display_name").alias("domain"),
          F.col("primary_topic").getField("field").getField("display_name").alias("field")
        ).alias("primary_topic"),
        F.col("referenced_works")
          .then(arr => F.transform(arr, workId => trimPrefix("https://openalex.org/")(workId)))
          .alias("referenced_works"),
        F.col("related_works")
          .then(arr => F.transform(arr, workId => trimPrefix("https://openalex.org/")(workId)))
          .alias("related_works"),
        F.col("keywords")
          .then(arr => F.transform(arr, keyword => keyword.getField("display_name")))
          .alias("keywords"),
        F.col("counts_by_year")
          .then(
            F.transform(
              _,
              citation =>
                F.struct(
                  citation
                    .getField("year")
                    .alias("year"),
                  citation
                    .getField("cited_by_count")
                    .alias("count")
                )
            )
          )
          .alias("citations")
      )
      .write
      .parquet("hdfs:///user/ubuntu/data/openalex")
  }
}
