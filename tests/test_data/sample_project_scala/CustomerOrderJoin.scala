package com.company.jobs

import org.apache.spark.sql.{SparkSession, DataFrame}

object CustomerOrderJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("customer-order-join").getOrCreate()
    run(spark)
  }

  def run(spark: SparkSession): Unit = {
    val orders = spark.read.parquet("s3://my-bucket/raw/orders/")
    val customers = spark.read.parquet("s3://my-bucket/raw/customers/")

    // Bug: CustomerHelper.validate() assumes email is always present,
    // but some records have null emails, causing a NullPointerException.
    val validated = customers.filter(row => CustomerHelper.validate(row))

    val joined = orders.join(validated, Seq("customer_id"), "left")
    joined.write.mode("overwrite").parquet("s3://my-bucket/output/joined/")
  }
}
