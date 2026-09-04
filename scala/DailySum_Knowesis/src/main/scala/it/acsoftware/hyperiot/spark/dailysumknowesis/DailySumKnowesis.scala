package it.acsoftware.hyperiot.spark.dailysumknowesis

import java.time.LocalDate
import cats.syntax.either._
import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
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

object DailySumKnowesis {

  // Recursive method to find all the subfolder of the given PATH
  def getFolders(fs: FileSystem, path: Path): Seq[String] = {
    val statuses = fs.listStatus(path)
    val folders = statuses.filter(_.isDirectory).map(_.getPath.toString)
    val subFolders = statuses.filter(_.isDirectory).flatMap(status => getFolders(fs, status.getPath))
    folders ++ subFolders
  }

  // Method to write JSON object into HBase, skipping the write if the row already exists unless overwrite is true
  def writeToHBase(rowKey: String, value: String, hBaseTable: org.apache.hadoop.hbase.client.Table, overwrite: Boolean): Unit = {
    val rowExists = hBaseTable.exists(new Get(Bytes.toBytes(rowKey)))
    if (rowExists && !overwrite) {
      println(s"Row with key $rowKey already exists, skipping write (overwrite=false)")
    } else {
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("value"), Bytes.toBytes("output"), Bytes.toBytes(value))
      hBaseTable.put(put)
    }
  }

  // Method used to concatenate rows of dataFrame into unique JSON object
  def concatenateRowsToJson(df: DataFrame, groupingFieldIds: Array[Long]): String = {

    val rows = df.collect().map(row => {
      // Estrai i valori per ciascun ID dall'array
      val groupingValues = groupingFieldIds.map(id => {
        val value = row.getAs[Any](id.toString)
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
      .appName( "DailySum")
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

    println("VALUES pre-sum")
    values.show()

    // Split dataframe in each column - il campo 0 e' il valore numerico da sommare, il campo 1 e' il timestamp
    val valueDF = values.select(col(hPacketFieldIds(0).toString).cast("double"))
    val timestampDF = values.select(col(hPacketFieldIds(1).toString))

    // Remove null values
    val nonNullValueDF = valueDF.na.drop()
    val nonNullTimestampDF = timestampDF.na.drop()

    nonNullTimestampDF.show()

    // Nome della colonna contenente l'epoch second UTC di inizio giornata, usata come chiave di raggruppamento
    val dateColumnName = hPacketFieldIds(1).toString

    // Conversione del timestamp (millisecondi) nell'epoch second UTC di inizio giornata,
    // usato poi per costruire la chiave HBase a tempo invertito (Long.MaxValue - epochSecondUTC)
    val nonNullTimestampFormattedDF = nonNullTimestampDF.withColumn(
      dateColumnName,
      (col(dateColumnName) / 1000 / 86400).cast("long") * 86400
    )

    val df1WithIndex = nonNullValueDF.withColumn("index", monotonically_increasing_id())
    val df2WithIndex = nonNullTimestampFormattedDF.withColumn("index", monotonically_increasing_id())

    // Merge dataframe using this fake index
    val resultDF = df1WithIndex
      .join(df2WithIndex, Seq("index"))
      .drop("index")  // after join, remove now useless column


    resultDF.show()

    // Somma il valore numerico per ciascuna data (una riga di output per giorno)
    val output = resultDF
      .groupBy(col(dateColumnName))
      .agg(sum(col(hPacketFieldIds(0).toString)).as("output"))
      .orderBy(col(dateColumnName))

    output.show()

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

    // Se true, una riga HBase gia' esistente per una data viene sovrascritta; se false viene lasciata invariata
    val overwrite = false

    // Elenco degli epoch second UTC (inizio giornata) distinti presenti nel risultato
    val distinctDates = output.select(dateColumnName).distinct().collect().map(_.getAs[Long](0))

    // Scrivi una riga HBase per ciascuna data, con chiave projectId_hProjectAlgorithmName_suffix
    // dove suffix = Long.MaxValue - epochMillisUTC, con padding a 19 cifre (lunghezza di Long.MaxValue)
    // cosi' l'ordinamento lessicografico delle chiavi HBase corrisponde a un ordinamento a tempo invertito,
    // come richiesto dal nuovo endpoint timerange che scandisce HBase per chiave numerica
    distinctDates.foreach { epochSecondUTC =>
      val dailyOutput = output.filter(col(dateColumnName) === epochSecondUTC)
      val jsonData = concatenateRowsToJson(dailyOutput, Array(hPacketFieldIds(1)))
      val epochMillisUTC = epochSecondUTC * 1000
      val invertedTimeSuffix = f"${Long.MaxValue - epochMillisUTC}%019d"
      val rowKey = projectId + "_" + hProjectAlgorithmName + "_" + invertedTimeSuffix
      writeToHBase(rowKey, jsonData, hBaseTable, overwrite)
    }

    // Chiudo connessione spark
    spark.stop()
  }

}
