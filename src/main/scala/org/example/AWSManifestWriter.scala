package org.example

import org.apache.spark.sql.SparkSession

import java.util.Calendar

object AWSManifestWriter {

  private val now = Calendar.getInstance().getTime

  def main(args: Array[String]): Unit = {

    println(s"Start time: $now")
    val spark = SessionFactory.getOrCreateSparkSession(Some("s3a"), reportsDir = "s3a://<your_bucket>/reports")
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.mapreduce.manifest.committer.summary.report.directory", "s3a://<your_bucket>/reports")


    val data = Seq(("John", "Doe", 30), ("Jane", "Doe", 25))
    val df = spark.createDataFrame(data).toDF("FirstName", "LastName", "Age")

    df.write
      .format("parquet")
      .mode("overwrite")
      .option("path", "s3a://<your_bucket>/test_manifest_writer1")
      .save()

    spark.stop()
    println(s"End time: $now")
  }
}
