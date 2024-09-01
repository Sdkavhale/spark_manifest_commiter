package org.example

import org.apache.spark.rdd.RDD

/*
* This is to test if the job fails while writing does spark cleans the partial data by itself
* */
object TestReliability extends App {

  val spark = SessionFactory.getOrCreateSparkSession(Some("gs"), master="local[2, 2]", reportsDir = "gs://<your_bucket>/reports")
  import spark.implicits._

  val data = Array(10,9,8,7,6)
  val distData: RDD[Int] = spark.sparkContext.parallelize(data)

  distData.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 6) {
        throw new Exception("Simulated task failure")
      }
      iter
    }.map(x => (x, x))
    .toDF("key", "value")
    .write.mode("append").parquet("gs://<your_bucket>/test_manifest_writer2")


  // Write the processed data to storage (append mode)
  val newRows = spark.read.parquet("gs://<your_bucket>/test_manifest_writer2").count()
  println(s"Number of rows written : $newRows")

}
