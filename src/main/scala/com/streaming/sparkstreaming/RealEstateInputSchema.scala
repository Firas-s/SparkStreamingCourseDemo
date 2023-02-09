package com.streaming.sparkstreaming

import org.apache.spark.sql.types._

object RealEstateInputSchema {
  def getInputSchema(): StructType = {
    val inputSchema = StructType(Array(
      StructField("street", StringType),
      StructField("city",StringType),
      StructField("zip",LongType),
      StructField("state",StringType),
      StructField("beds", IntegerType),
      StructField("baths", IntegerType),
      StructField("sq__ft", DoubleType),
      StructField("type", StringType),
      StructField("sale_date", StringType),
      StructField("price", DoubleType),
      StructField("latitude", DoubleType),
      StructField("longitude", DoubleType),
      StructField("sourceTime", LongType)
    ))
    inputSchema
  }

}
