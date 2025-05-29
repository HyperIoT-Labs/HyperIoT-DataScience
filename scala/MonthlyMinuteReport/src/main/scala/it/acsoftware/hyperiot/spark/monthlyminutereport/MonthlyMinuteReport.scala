package it.acsoftware.hyperiot.spark.monthlyminutereport

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

object MonthlyMinuteReport {

  // Funzione ricorsiva per cercare tutte le cartelle nel percorso specificato
  def getFolders(fs: FileSystem, path: Path): Seq[String] = {
    val statuses = fs.listStatus(path)
    val folders = statuses.filter(_.isDirectory).map(_.getPath.toString)
    val subFolders = statuses.filter(_.isDirectory).flatMap(status => getFolders(fs, status.getPath))
    folders ++ subFolders
  }

  // Funzione per scrivere l'oggetto in HBase
  def writeToHBase(rowKey: String, value: String, hBaseTable: org.apache.hadoop.hbase.client.Table): Unit = {
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("value"), Bytes.toBytes("output"), Bytes.toBytes(value))
    hBaseTable.put(put)
  }

  // Funzione per concatenare le righe del DataFrame in un unico oggetto JSON
  def concatenateRowsToJson(df: DataFrame, hPacketFieldId: Long): String = {
    // Estrai tutte le righe come sequenza di mappe
    val rows = df.collect().map(row => {
      val valueId  = row.getAs[String]("value")
      val year  = row.getAs[Int]("year")
      val month  = row.getAs[Int]("month")
      val output = row.getAs[Double]("total_minutes")

      Map("grouping" -> Map(hPacketFieldId -> valueId, "year" -> year, "month" -> month), "output" -> output)
    })

    val headers = List("id", "year", "month")

    // Crea un oggetto JSON con tutte le righe
    implicit val formats = Serialization.formats(NoTypeHints)
    Serialization.write(Map("results" -> rows, "headers" -> headers, "customSchema" -> true))
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
     * This variable contains job configuration
     */
    val jobConfig: Json = parse(args(4)).getOrElse(Json.Null)

    /*
     TODO framework issue - Validate jobConfig (i.e. it has one input and one output at least, and so on).
      Doing so, you are sure values such as hPacketId and hPacketFieldId exist
    */

    // get first HPacket ID
    val hPacketId = root.input.each.packetId.long.getAll(jobConfig).headOption.get

    // get first HPacketField ID (the one who groups)
    val hPacketFieldId = root.input.each.mappedInputList.each.packetFieldId.long.getAll(jobConfig).headOption.get

    // get first HPacketField type
    var hPacketFieldType =
      root.input.each.mappedInputList.each.algorithmInput.fieldType.string.getAll(jobConfig).headOption.get.toLowerCase()
    // one of input type can be "number". However, SparkSQL cannot cast to number, but it does to decimal
    hPacketFieldType = if (hPacketFieldType == "number") "decimal" else hPacketFieldType

    val outputName = root.output.each.name.string.getAll(jobConfig).headOption.get

    // Get the hpacketFieldId of startDate and endDate
    val startDateFieldId = root.input.each.mappedInputList.each.packetFieldId.long.getAll(jobConfig).drop(1).headOption.get
    val endDateFieldId = root.input.each.mappedInputList.each.packetFieldId.long.getAll(jobConfig).drop(2).headOption.get

    val path = hdfsBasePath + "/" + hPacketId //ALL FILES .AVRO

    // Ottieni il FileSystem per il percorso HDFS
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", fsDefaultFs)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Ottieni la lista di tutte le cartelle nel percorso HDFS
    val allFolders = getFolders(fs, new Path(path))

    // Crea un ArrayBuffer per memorizzare i percorsi di tutti i file Avro
    val avroFilesBuffer = ArrayBuffer[String]()

    // Per ogni sottocartella, ottieni la lista di file Avro e aggiungili all'ArrayBuffer
    allFolders.foreach { folder =>
      val avroFiles = fs.listStatus(new Path(folder))
        .filter(_.getPath.getName.endsWith(".avro"))
        .map(_.getPath.toString)
      avroFilesBuffer ++= avroFiles
    }

    // Converti l'ArrayBuffer in una sequenza immutabile
    val avroFiles = avroFilesBuffer.toSeq

    // Leggi i file Avro uno ad uno e crea i DataFrame corrispondenti
    val dfs: Seq[DataFrame] = avroFiles.map { file =>

      try {
          val df = spark.read.format("avro").load(file)

          df.transform { df =>
            df.select(
              explode(map_values(col("fields"))).as("hPacketField")
            )
            .filter(
              col("hPacketField.id") === hPacketFieldId ||
              col("hPacketField.id") === startDateFieldId ||
              col("hPacketField.id") === endDateFieldId
            )
            .select(
              when(col("hPacketField.id") === hPacketFieldId, coalesce(                                                                             // coalesce
                col("hPacketField.value.member0").cast("string"), 
                col("hPacketField.value.member1").cast("string"),
                col("hPacketField.value.member2").cast("string"), 
                col("hPacketField.value.member3").cast("string"),
                col("hPacketField.value.member4").cast("string"), 
                col("hPacketField.value.member5").cast("string"))).as("value"),
              when(col("hPacketField.id") === startDateFieldId, col("hPacketField.value.member1")).as("startDate"),
              when(col("hPacketField.id") === endDateFieldId, col("hPacketField.value.member1")).as("endDate")
            )
          }

      } catch {
            case ex: Throwable => 
              println("Exception: " + ex.getMessage)
              spark.emptyDataFrame // Ritorna un DataFrame vuoto in caso di eccezione
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

    // Check empty dataframe
    if (values.columns.isEmpty || values.columns.length != 3) { println("Values dataframe is empty!") } 
    // normal flow
    else {
      // Split dataframe in each column
      val valueDF = values.select(col("value").cast("string"))
      val startDateDF = values.select(col("startDate"))
      val endDateDF = values.select(col("endDate"))

      // Remove null values
      val nonNullValueDF = valueDF.na.drop()
      val nonNullStartDateDF = startDateDF.na.drop()
      val nonNullEndDateDF = endDateDF.na.drop()

      // Add dummy index column
      val df1WithIndex = nonNullValueDF.withColumn("index", monotonically_increasing_id())
      val df2WithIndex = nonNullStartDateDF.withColumn("index", monotonically_increasing_id())
      val df3WithIndex = nonNullEndDateDF.withColumn("index", monotonically_increasing_id())

      // Merge dataframe using this fake index
      val resultDF = df1WithIndex
        .join(df2WithIndex, Seq("index"))
        .join(df3WithIndex, Seq("index"))
        .drop("index")  // after join, remove now useless column

      // Compute tot minutes
      val dfWithDuration = resultDF.withColumn("duration", (col("endDate") - col("startDate")) / 60000) // Converte in minuti
      val dfWithTimestamp = dfWithDuration.withColumn("startTimestamp", (col("startDate") / 1000).cast("timestamp"))

      val dfWithMonthYear = dfWithTimestamp
          .withColumn("year", year(col("startTimestamp")))
          .withColumn("month", month(col("startTimestamp")))

      // Raggruppa per value, anno e mese, e somma la duration
      val result = dfWithMonthYear
        .groupBy("value", "year", "month")
        .agg(
          sum("duration").alias("total_minutes")
        )
        .withColumn("timestamp", current_timestamp().cast("long"))
        .orderBy("value", "year", "month")

      result.show()

      // Retrieve timestamp
      val timestampValue = result.select("timestamp").first().getLong(0)

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
      val jsonData = concatenateRowsToJson(result, hPacketFieldId)

      // Make key for HBaseTable
      val rowKey = projectId + "_" + hProjectAlgorithmName + "_" + (Long.MaxValue - timestampValue.asInstanceOf[Long])

      // Write on HBase
      writeToHBase(rowKey, jsonData, hBaseTable)
      
    }

    // Close spark connection
    spark.stop()
  }

}
