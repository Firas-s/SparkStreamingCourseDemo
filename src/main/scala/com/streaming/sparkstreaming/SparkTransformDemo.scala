package com.streaming.sparkstreaming

import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkTransformDemo {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    Logger.getRootLogger.setLevel(Level.INFO)

    val conf = new SparkConf().setAppName("Spark_Transform").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val input = sparkSession.readStream
      .format(source="kafka")
      .option("kafka.bootstrap.servers","34.123.230.68:9092")
      .option("subscribe","real_estate_transactions_source")
      .option("startingOffsets","lastest").load()

    val inputSchema = RealEstateInputSchema.getInputSchema()

    val parseInputData: Dataset[org.apache.spark.sql.Row]=
      input.selectExpr(exprs= "CAST(key AS STRING)","CAST(value AS STRING)")
        .withColumn(colName = "kafkaValueJson",from_json(col(colName ="value"),inputSchema))
        .select(col(colName ="kafkaValueJson.*"))

    parseInputData.createOrReplaceTempView(viewName ="parseInputData")

    val transformedData =sparkSession.sql(getTransformationSql())

    val output = transformedData
      .select(to_json(struct(col(colName = "*"))).alias(alias ="value"))
      .writeStream
      .format(source = "kafka")
      .option("kafka.bootstrap.servers","34.123.230.68:9092")
      .option("topic","real_estate_transformed_spark")
      .option("CheckpointLocation","/tmp/SparkTranformDemo")
      .trigger(Trigger.ProcessingTime("1 seconds"))

    sparkSession.streams.awaitAnyTermination( )
  }
  def getTransformationSql(): String = {
    """
        Select lower(city) as city,
        zip,
        case when state ='CA' then 'California' else state end as state,
        beds,
        baths,
        type,
        sale_date as saleDate,
        price,
        latitude,
        longitude,
        sourceTime,
        case when beds is null or beds = 0 then 'NA'
             when beds = 1 then 'Studio'
           when beds = 2 then 'bhk'
             else 'PENT_HOUSE' end as houseType
        from parseInputData
        """
      .stripMargin

  }

}