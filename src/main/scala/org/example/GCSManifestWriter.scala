package org.example

import org.apache.spark.sql.SparkSession

import java.util.Calendar

object GCSManifestWriter {

  private val now = Calendar.getInstance().getTime

  def main(args: Array[String]): Unit = {

    val startTime = now
    val spark = SessionFactory.getOrCreateSparkSession(Some("gs"), master="local[4, 4]", reportsDir = "gs://<your_bucket>/reports")
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.mapreduce.manifest.committer.summary.report.directory", "gs://<your_bucket>/reports")

//    val data = Seq(("John", "Doe", 30), ("Jane", "Doe", 25))
//    val df = spark.createDataFrame(data).toDF("FirstName", "LastName", "Age")
//
//    df.write
//      .format("parquet")
//      .mode("overwrite")
//      .option("path", "gs://<your_bucket>/test_manifest_writer1")
//      .save()
    import spark.implicits._

    spark.range(10000).repartition(2)
      .map { i =>
        if (i == 9999) { Thread.sleep(5000); throw new RuntimeException("oops!") }
        else i
      }
      .write.mode("append").parquet("gs://<your_bucket>/test_manifest_writer1")

    // Compare the number of newly added rows from the failed jobs
    val newRows = spark.read.parquet("gs://<your_bucket>/test_manifest_writer1").count() - 10000
    println(s"Number of rows written : $newRows")

    spark.stop()
    val executionTime = now.getTime - startTime.getTime
    println(s"Execution Time: $executionTime")
  }
}
