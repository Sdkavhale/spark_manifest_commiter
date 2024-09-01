package org.example

import org.apache.spark.sql.SparkSession

object SessionFactory {

  // For authentication we have to export GOOGLE_APPLICATION_CREDENTIALS=/Users/application_default_credentials.json and similar for other clouds
  def getOrCreateSparkSession(fsScheme: Option[String], reportsDir: String, master: String = "local"): SparkSession = {

    val configMap = fsScheme.getOrElse("gs").toLowerCase match {
      case "s3a" =>
        Map("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a" -> "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
          "fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
          "fs.AbstractFileSystem.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3A")

      case "abfs" =>
        Map("spark.hadoop.mapreduce.outputcommitter.factory.scheme.abfs" -> "org.apache.hadoop.fs.azurebfs.commit.AzureManifestCommitterFactory",
          "fs.abfs.impl" -> "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
          "fs.AbstractFileSystem.abfs.impl" -> "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")

      case _ =>
        Map("spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs" -> "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory",
          "fs.gs.impl"-> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
          "fs.AbstractFileSystem.gs.impl"-> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    }

    val spark = SparkSession.builder()
      .appName(s"$fsScheme manifest writer")
      .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      .config("spark.hadoop.mapreduce.manifest.committer.summary.report.directory", reportsDir)
      .config("spark.hadoop.mapreduce.manifest.committer.io.threads", "200")
      .config("spark.task.maxFailures", "4")

    configMap.foreach { case (k, v) => spark.config(k, v) }

    spark
      .master(master)
      .getOrCreate()
  }

}
