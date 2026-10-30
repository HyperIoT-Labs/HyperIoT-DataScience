package it.acsoftware.hyperiot.spark.weeklycountby

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

object WeeklyCountBy {

  // Lunedi' della prima settimana da conteggiare (yyyy-MM-dd)
  val FirstWeekStart = "2026-03-23"

  // Recursive method to find all the subfolder of the given PATH
  def getFolders(fs: FileSystem, path: Path): Seq[String] = {
    val statuses = fs.listStatus(path)
    val folders = statuses.filter(_.isDirectory).map(_.getPath.toString)
    val subFolders = statuses.filter(_.isDirectory).flatMap(status => getFolders(fs, status.getPath))
    folders ++ subFolders
  }

  // Method to write JSON object into HBase
  def writeToHBase(rowKey: String, value: String, hBaseTable: org.apache.hadoop.hbase.client.Table): Unit = {
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("value"), Bytes.toBytes("output"), Bytes.toBytes(value))
    hBaseTable.put(put)
  }

  // Method used to concatenate rows of dataFrame into unique JSON object
  def concatenateRowsToJson(df: DataFrame, groupByFieldId: Long): String = {

    // Solo colonne string/long: collect di colonne DateType sul driver richiede --add-opens su sun.util.calendar
    val rows = df.select(groupByFieldId.toString, "start_date", "end_date", "count").collect().map(row => {
      Map(
        "group_by_id" -> row.getAs[String](groupByFieldId.toString),
        "start_date" -> row.getAs[String]("start_date"),
        "end_date" -> row.getAs[String]("end_date"),
        "count" -> row.getAs[Long]("count")
      )
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
      .appName( "WeeklyCountBy")
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

    // Extract the array ids which have to be grouped
    val hPacketFieldIds: List[Long] = root.input.each.mappedInputList.each.packetFieldId.long.getAll(jobConfig)

    // get first HPacket ID
    val hPacketId = root.input.each.packetId.long.getAll(jobConfig).headOption.get

    // get the output name
    val outputName = root.output.each.name.string.getAll(jobConfig).headOption.get

    // TODO - framework issue - as many paths as hpackets inside input configuration. After that, how many dataframes do we have? ...
    // TODO: ... one for each path or one containing all hpackets?
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

        // Una riga per pacchetto, una colonna per ogni 'hPacketField.id': valore e timestamp restano nella stessa riga
        val selectedCols = hPacketFieldIds.map { id =>
          val hPacketField = filter(map_values(col("fields")), f => f("id") === id).getItem(0)
          coalesce(
            hPacketField.getField("value").getField("member0").cast("string"),
            hPacketField.getField("value").getField("member1").cast("string"),
            hPacketField.getField("value").getField("member2").cast("string"),
            hPacketField.getField("value").getField("member3").cast("string"),
            hPacketField.getField("value").getField("member4").cast("string"),
            hPacketField.getField("value").getField("member5").cast("string")
          ).as(s"$id")
        }

        // Risultato finale: solo le colonne dinamiche
        val resultDf = df.select(selectedCols: _*)

        // Mostra il risultato
        resultDf.show()

        resultDf

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

    // Unisce i DataFrame in uno unico
    val values: DataFrame = dfsWithUnifiedSchema.reduce(_.union(_))

    println("VALUES pre-count")
    values.show()

    // Valore e timestamp dello stesso pacchetto; scarta i pacchetti in cui manca uno dei due
    val resultDF = values
      .select(col(hPacketFieldIds(0).toString).cast("string"), col(hPacketFieldIds(1).toString))
      .na.drop()

    resultDF.show()

    val groupByCol = hPacketFieldIds(0).toString
    val timestampCol = hPacketFieldIds(1).toString

    // Settimane lunedi'-domenica a partire da lunedi' 23/03/2026 (end_date = lunedi' successivo, escluso)
    val weeklyCounts = resultDF
      .withColumn("week_start", to_date(date_trunc("week", (col(timestampCol) / 1000).cast("timestamp"))))
      .filter(col("week_start") >= to_date(lit(FirstWeekStart)))
      .groupBy(col(groupByCol), col("week_start"))
      .count()

    // Calendario completo: ogni group_by_id per ogni settimana fino a quella corrente (count = 0 se senza dati)
    val weeks = spark.range(1).select(explode(expr(
      s"sequence(to_date('$FirstWeekStart'), to_date(date_trunc('week', current_date())), interval 1 week)"
    )).as("week_start"))
    val groups = resultDF.select(col(groupByCol)).distinct()

    val output = groups.crossJoin(weeks)
      .join(weeklyCounts, Seq(groupByCol, "week_start"), "left")
      .withColumn("count", coalesce(col("count"), lit(0L)))
      .withColumn("start_date", date_format(col("week_start"), "dd-MM-yyyy"))
      .withColumn("end_date", date_format(date_add(col("week_start"), 7), "dd-MM-yyyy"))
      .withColumn("timestamp", current_timestamp().cast("long"))
      .orderBy(col(groupByCol), col("week_start"))

    output.show()

    // Retrieve timestamp
    val timestampValue = output.select("timestamp").first().getLong(0)

    // write output to HBase
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

    // Chiamata alla funzione per concatenare le righe del DataFrame in un unico oggetto JSON
    val jsonData = concatenateRowsToJson(output, hPacketFieldIds(0))

    // Genera la chiave univoca per la riga
    val rowKey = projectId + "_" + hProjectAlgorithmName + "_" + (Long.MaxValue - timestampValue.asInstanceOf[Long])

    // Scrivi l'oggetto JSON in HBase
    writeToHBase(rowKey, jsonData, hBaseTable)

    // Chiudo connessione spark
    spark.stop()
  }

}