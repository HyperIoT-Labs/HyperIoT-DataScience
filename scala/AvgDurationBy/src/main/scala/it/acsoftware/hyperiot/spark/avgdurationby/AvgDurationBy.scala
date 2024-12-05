package it.acsoftware.hyperiot.spark.avgdurationby

import java.time.LocalDate
import cats.syntax.either._
import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

import org.json4s._
import org.json4s.jackson.Serialization

import java.time.Instant
import org.apache.spark.sql.functions.monotonically_increasing_id

object AvgDurationBy {

  // Recursive method to find all folder path
  def getFolders(fs: FileSystem, path: Path): Seq[String] = {
    val statuses = fs.listStatus(path)
    val folders = statuses.filter(_.isDirectory).map(_.getPath.toString)
    val subFolders = statuses.filter(_.isDirectory).flatMap(status => getFolders(fs, status.getPath))
    folders ++ subFolders
  }

  // Method to write objects into HBase
  def writeToHBase(rowKey: String, value: String, hBaseTable: org.apache.hadoop.hbase.client.Table): Unit = {
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("value"), Bytes.toBytes("output"), Bytes.toBytes(value))
    hBaseTable.put(put)
  }

  // Method used to concatenate rows of dataFrame into unique JSON object
  def concatenateRowsToJson(df: DataFrame, hPacketFieldIds: Array[Long]): String = {
    
    val rows = df.collect().map(row => {
      // Estrai i valori per ciascun ID dall'array
      val groupingValues = hPacketFieldIds.map(id => {
        val value = row.getAs[String](id.toString)
        (id -> value)
      }).toMap
      val output = row.getAs[Double]("output")
      Map("grouping" -> groupingValues, "output" -> output)
    })

    // Crea un oggetto JSON con tutte le righe
    implicit val formats = Serialization.formats(NoTypeHints)
    Serialization.write(Map("results" -> rows))
  }

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .config("spark.executor.extraJavaOptions", 
        "--illegal-access=permit --add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.io=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.net=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.util=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
      .config("spark.driver.extraJavaOptions", 
        "--illegal-access=permit --add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.io=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.net=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.util=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
        "--illegal-access=permit --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
      .appName( "AvgDurationBy")
      .getOrCreate()

    /**
     * Project ID
     */
    val projectId = args(0)
    /**
     * Algorithm ID
     */
    val algorithmId = args(1)
    /**
     * HProjectAlgorithm name
     */
    val hProjectAlgorithmName = args(2)

    /**
     * This variable contains hdfs and hbase configuration
     */
    val hadoopConfig: Json = parse(args(3)).getOrElse(Json.Null)

    val fsDefaultFs = root.fsDefaultFs.string.getOption(hadoopConfig).get
    val hdfsWriteDir = root.hdfsWriteDir.string.getOption(hadoopConfig).get
    val hdfsBasePath = fsDefaultFs + hdfsWriteDir

    /**
     * This variable contains job configuration (input, groupBy, output)
     */
    val jobConfig: Json = parse(args(4)).getOrElse(Json.Null)

    // get the HPacket Id
    val hPacketId = root.input.each.packetId.long.getAll(jobConfig).headOption.get

    // Extract the INPUT ID array and the GROUPBY array
    val hPacketFieldIds: List[Long] = root.input.each.mappedInputList.each.packetFieldId.long.getAll(jobConfig)
    val groupByIds: List[Long] = root.input.each.groupBy.each.id.long.getAll(jobConfig)

    // Get the ID for START DATE and END DATE (Only for duration)
    val len = hPacketFieldIds.length
    val startDateId = hPacketFieldIds(len - 2)
    val endDateId = hPacketFieldIds(len - 1)
    
    val outputName = root.output.each.name.string.getAll(jobConfig).headOption.get

    val path = hdfsBasePath + "/" + hPacketId //ALL FILES .AVRO

    // Get HDFS Filestystem and path
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", fsDefaultFs)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Get all folders inside HDFS path
    val allFolders = getFolders(fs, new Path(path))

    // ArrayBuffer to save all path and all avro files
    val avroFilesBuffer = ArrayBuffer[String]()

    // For every sub-folder, get avro file list and add it to ArrayBuffer
    allFolders.foreach { folder =>
      val avroFiles = fs.listStatus(new Path(folder))
        .filter(_.getPath.getName.endsWith(".avro"))
        .map(_.getPath.toString)
      avroFilesBuffer ++= avroFiles
    }

    // Convert ArrayBuffer into seq
    val avroFiles = avroFilesBuffer.toSeq

    // Read avro file 1 by 1 and make relative DataFrame
    val dfs: Seq[DataFrame] = avroFiles.map { file =>

      try {
          val df = spark.read.format("avro").load(file)

          df.select(explode(map_values(col("fields"))).as("hPacketField"))  
          .filter(col("hPacketField.id").isin(hPacketFieldIds:_*) || col("hPacketField.id").isin(groupByIds: _*))                      // get target HPacketFields // todo - framework issue: === must be something like IN(hPacketFieldIds)
          .groupBy().pivot("hPacketField.id").agg(first(  
                coalesce(                                                                             // coalesce
                col("hPacketField.value.member0").cast("string"), 
                col("hPacketField.value.member1").cast("string"),
                col("hPacketField.value.member2").cast("string"), 
                col("hPacketField.value.member3").cast("string"),
                col("hPacketField.value.member4").cast("string"), 
                col("hPacketField.value.member5").cast("string")))
              )
      } catch {
            case ex: Throwable => 
              println("Exception: " + ex.getMessage)
              spark.emptyDataFrame // Empty dataframe while exception
      }
    }

    val schemas = dfs.map(_.schema)
    val unifiedSchema = schemas.reduce((schema1, schema2) => StructType(schema1.fields ++ schema2.fields))

    val dfsWithUnifiedSchema = dfs.map(df => {
      val missingColumns = unifiedSchema.fieldNames.toSet.diff(df.columns.toSet)
      missingColumns.foldLeft(df)((acc, colName) => acc.withColumn(colName, lit(null)))
    })

    // Merge dataframe
    val values: DataFrame = dfsWithUnifiedSchema.reduce(_.union(_))

    /*
      Example of final output:
      +----+-------------------+----------+
      |6445|             output| timestamp|
      +----+-------------------+----------+
      |null|               null|1717073415|
      |   1|                0.0|1717073415|
      |   0|            0.05615|1717073415|
      |  47| 0.4509111111111111|1717073415|
      |  21| 0.6469833333333334|1717073415|
      | 115|0.10987222222222222|1717073415|
      |  95| 1.6050666666666666|1717073415|
      |  20| 0.4017833333333333|1717073415|
      | 116|0.28926666666666667|1717073415|
      +----+-------------------+----------+
    */

    // Rename startDate and EndDate column
    val resultDF = values.withColumnRenamed(startDateId.toString, "startDate")
          .withColumnRenamed(endDateId.toString, "endDate")

    // Compute avg duration
    val dfWithDuration = resultDF.withColumn("duration", (col("endDate") - col("startDate")) / 60000) // Converte in minuti
    

    // Compute aggregate/grouped statistics
    val avgDurationByAssetId = dfWithDuration
      .groupBy(groupByIds.map(id => col(id.toString)): _*)
      .agg(avg("duration").as("output"))
      .withColumn("timestamp", current_timestamp().cast("long"))

    // Retrieve timestamp
    val timestampValue = avgDurationByAssetId.select("timestamp").first().getLong(0)

    // Write output to HBase
    val conf = HBaseConfiguration.create()
    conf.set("hbase.rootdir", root.hbaseRootdir.string.getOption(hadoopConfig).get)
    conf.set("hbase.master.port", root.hbaseMasterPort.string.getOption(hadoopConfig).get)
    conf.set("hbase.cluster.distributed", root.hbaseClusterDistributed.string.getOption(hadoopConfig).get)
    conf.set("hbase.regionserver.info.port", root.hbaseRegionserverInfoPort.string.getOption(hadoopConfig).get)
    conf.set("hbase.master.info.port", root.hbaseMasterInfoPort.string.getOption(hadoopConfig).get)
    conf.set("hbase.zookeeper.quorum", root.hbaseZookeeperQuorum.string.getOption(hadoopConfig).get)
    conf.set("hbase.master", root.hbaseMaster.string.getOption(hadoopConfig).get)
    conf.set("hbase.regionserver.port", root.hbaseRegionserverPort.string.getOption(hadoopConfig).get)
    conf.set("hbase.master.hostname", root.hbaseMasterHostname.string.getOption(hadoopConfig).get)

    val conn = ConnectionFactory.createConnection(conf)
    val tableName = "algorithm" + "_" + algorithmId
    val table = TableName.valueOf(tableName)
    val hBaseTable = conn.getTable(table)

    // Create JSON file
    val jsonData = concatenateRowsToJson(avgDurationByAssetId, groupByIds.toArray)

    // Make key for HBaseTable
    val rowKey = projectId + "_" + hProjectAlgorithmName + "_" + (Long.MaxValue - timestampValue.asInstanceOf[Long])

    // Write on HBase
    writeToHBase(rowKey, jsonData, hBaseTable)

    // Close spark connection
    spark.stop()
  }

}