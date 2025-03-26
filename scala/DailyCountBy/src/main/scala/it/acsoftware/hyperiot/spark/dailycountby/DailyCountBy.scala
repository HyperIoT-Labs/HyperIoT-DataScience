package it.acsoftware.hyperiot.spark.dailycountby

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

object DailyCountBy {

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
  def concatenateRowsToJson(df: DataFrame, hPacketFieldIds: Array[Long]): String = {
    
    val rows = df.collect().map(row => {
      // Estrai i valori per ciascun ID dall'array
      val groupingValues = hPacketFieldIds.map(id => {
        val value = row.getAs[String](id.toString)
        (id -> value)
      }).toMap
      val output = row.getAs[Long]("output")
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
      .appName( "DailyCountBy")
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

        val transformedDf = df.select(explode(map_values(col("fields"))).as("hPacketField"))
          .filter(col("hPacketField.id").isin(hPacketFieldIds: _*))  // Filtra per gli ID
          .select(
            col("hPacketField.id"),
            coalesce(
              col("hPacketField.value.member0").cast("string"),
              col("hPacketField.value.member1").cast("string"),
              col("hPacketField.value.member2").cast("string"),
              col("hPacketField.value.member3").cast("string"),
              col("hPacketField.value.member4").cast("string"),
              col("hPacketField.value.member5").cast("string")
            ).as("value")
          )

        // Creazione dinamica delle colonne per ogni 'hPacketField.id'
        var finalDf = transformedDf
        hPacketFieldIds.foreach { id =>
          finalDf = finalDf.withColumn(s"$id", when(col("id") === id, col("value")).otherwise(lit(null)))
        }

        // Seleziona solo le colonne che abbiamo creato dinamicamente (senza 'id' o altre colonne non necessarie)
        val selectedCols = hPacketFieldIds.map(id => s"$id")

        // Risultato finale: solo le colonne dinamiche
        val resultDf = finalDf.select(selectedCols.head, selectedCols.tail: _*)

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

    // Split dataframe in each column
    val valueDF = values.select(col(hPacketFieldIds(0).toString).cast("string"))
    val timestampDF = values.select(col(hPacketFieldIds(1).toString))

    // Remove null values
    val nonNullValueDF = valueDF.na.drop()
    val nonNullTimestampDF = timestampDF.na.drop()

    nonNullTimestampDF.show()

    // Conversione in data nel formato gg/MM/yyyy
    val nonNullTimestampFormattedDF = nonNullTimestampDF.withColumn(
      hPacketFieldIds(1).toString,
      date_format((col(hPacketFieldIds(1).toString) / 1000).cast("timestamp"), "dd/MM/yyyy")
    )

    val df1WithIndex = nonNullValueDF.withColumn("index", monotonically_increasing_id())
    val df2WithIndex = nonNullTimestampFormattedDF.withColumn("index", monotonically_increasing_id())

    // Merge dataframe using this fake index
    val resultDF = df1WithIndex
      .join(df2WithIndex, Seq("index"))
      .drop("index")  // after join, remove now useless column


    resultDF.show()

    val output = resultDF
      .groupBy(hPacketFieldIds.map(id => col(id.toString)): _*)
      .count()
      .withColumnRenamed("count", "output")
      .withColumn("timestamp", current_timestamp().cast("long"))
      .orderBy(hPacketFieldIds.map(id => col(id.toString)): _*) // Ordina per le colonne di raggruppamento

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
    val jsonData = concatenateRowsToJson(output, hPacketFieldIds.toArray)

    // Genera la chiave univoca per la riga
    val rowKey = projectId + "_" + hProjectAlgorithmName + "_" + (Long.MaxValue - timestampValue.asInstanceOf[Long])

    // Scrivi l'oggetto JSON in HBase
    writeToHBase(rowKey, jsonData, hBaseTable)

    // Chiudo connessione spark
    spark.stop()
  }

}