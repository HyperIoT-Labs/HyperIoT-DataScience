package it.acsoftware.hyperiot.spark.dailyhourreportknowesis

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
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.jackson.Serialization
import java.time.Instant

object DailyHourReportKnowesis {

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
  def concatenateRowsToJson(df: DataFrame, dateColumnName: String): String = {

    val rows = df.collect().map(row => {
      val epochSecondUTC = row.getAs[Long](dateColumnName)
      val output = row.getAs[Double]("output")

      Map("grouping" -> Map("date" -> epochSecondUTC), "output" -> output)
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
      .appName( "DailyHourReport")
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

    // get the HPacketField ID of the cumulative hour counter (unico campo mappato, nessuna dimensione di raggruppamento)
    val hPacketFieldId = root.input.each.mappedInputList.each.packetFieldId.long.getAll(jobConfig).headOption.get

    val outputName = root.output.each.name.string.getAll(jobConfig).headOption.get

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

        // Aggiungo un id di evento stabile PRIMA dell'explode, cosi' posso ricompattare
        // correttamente i campi esplosi appartenenti allo stesso evento (vedi piu' sotto)
        val df = spark.read.format("avro").load(file)
          .withColumn("__event_id", monotonically_increasing_id())

        val transformedDf = df.select(col("__event_id"), explode(map_values(col("fields"))).as("hPacketField"))
          .filter(
            col("hPacketField.id") === hPacketFieldId ||
            col("hPacketField.id") === 0  // id 0 = campo built-in timestamp del packet
          )
          .select(
            col("__event_id"),
            when(col("hPacketField.id") === hPacketFieldId, coalesce(
              col("hPacketField.value.member0").cast("string"),
              col("hPacketField.value.member1").cast("string"),
              col("hPacketField.value.member2").cast("string"),
              col("hPacketField.value.member3").cast("string"),
              col("hPacketField.value.member4").cast("string"),
              col("hPacketField.value.member5").cast("string"))).as("counterValue"),
            when(col("hPacketField.id") === 0, col("hPacketField.value.member1")).as("timestamp")
          )

        // Un singolo evento genera una riga esplosa per ogni field id trovato al suo interno (counterValue/timestamp):
        // le ricompatto in un'unica riga per evento (raggruppando per __event_id), cosi' i valori
        // dei diversi campi restano associati allo stesso evento originale invece di finire
        // su righe separate con gli altri campi a null.
        val resultDf = transformedDf
          .groupBy("__event_id")
          .agg(
            first(col("counterValue"), ignoreNulls = true).as("counterValue"),
            first(col("timestamp"), ignoreNulls = true).as("timestamp")
          )
          .drop("__event_id")

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

    println("VALUES pre-report")
    values.show()

    // Check empty dataframe
    if (values.columns.isEmpty || values.columns.length != 2) {
      println("Values dataframe is empty!")
    }
    // normal flow
    else {

      // Tieni solo le letture che hanno sia il valore del contatore sia il timestamp: ogni riga di
      // "values" rappresenta gia' un'unica lettura con entrambi i campi valorizzati nella stessa riga
      // (vedi il collapse per __event_id fatto sopra durante la lettura dei file), quindi non serve
      // piu' ricostruire l'accoppiamento con un indice fittizio.
      val nonNullValues = values.na.drop(Seq("counterValue", "timestamp"))

      nonNullValues.show()

      // Nome della colonna contenente l'epoch second UTC di inizio giornata, usata come chiave di
      // raggruppamento e come base per la chiave HBase
      val dateColumnName = "date"

      val dfWithDayEpoch = nonNullValues
        .withColumn(dateColumnName, (col("timestamp") / 1000 / 86400).cast("long") * 86400)
        .withColumn("counterValue", col("counterValue").cast("double"))
        // Timestamp vero e proprio (non solo il giorno) per l'ordinamento cronologico delle letture
        .withColumn("readingTimestamp", (col("timestamp") / 1000).cast("timestamp"))

      // Finestra globale ordinata per timestamp decrescente: TC_H e' un contatore cumulativo delle ore
      // macchina, sempre crescente finche' la macchina resta accesa. Ad ogni lettura confrontiamo il
      // valore corrente con quello della lettura precedente (nella stessa finestra):
      // - se il valore e' inferiore al precedente, la macchina si e' riavviata e il contatore si e'
      //   azzerato: il delta da contare e' il valore corrente stesso (si riparte "da zero")
      // - altrimenti il delta e' la differenza rispetto alla lettura precedente
      // Stessa identica logica di MonthlyHourReport, solo raggruppata per giorno invece che per mese.
      val globalWindowSpec = Window.orderBy(desc("readingTimestamp"))

      val dfWithLag = dfWithDayEpoch
        .withColumn("prevCounterValue", lag("counterValue", 1).over(globalWindowSpec))

      val dfWithFirstRowFlag = dfWithLag
        .withColumn("isFirstRow", when(row_number().over(globalWindowSpec) === 1, lit(1)).otherwise(lit(0)))

      val dfWithDiff = dfWithFirstRowFlag
        .withColumn("counterDiff",
          when(col("isFirstRow") === 1,
            col("counterValue")
          ).otherwise(
                when(col("counterValue") < col("prevCounterValue"), col("counterValue"))
                  .otherwise(col("counterValue") - col("prevCounterValue"))
              ))

      // Raggruppa per giorno e somma i delta (ore): una riga di output per ciascun giorno, cosi'
      // come per DailyCountByKnowesis/DailySumKnowesis
      val output = dfWithDiff
        .groupBy(col(dateColumnName))
        .agg(sum("counterDiff").alias("output"))
        .orderBy(col(dateColumnName))

      output.show()

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

      // Se true, una riga HBase gia' esistente per una data viene sovrascritta; se false viene lasciata invariata
      val overwrite = false

      // Elenco degli epoch second UTC (inizio giornata) distinti presenti nel risultato
      val distinctDates = output.select(dateColumnName).distinct().collect().map(_.getAs[Long](0))

      // Scrivi una riga HBase per ciascuna data, con chiave projectId_hProjectAlgorithmName_suffix
      // dove suffix = Long.MaxValue - epochSecondUTC, con padding a 19 cifre (lunghezza di Long.MaxValue)
      // cosi' l'ordinamento lessicografico delle chiavi HBase corrisponde a un ordinamento a tempo invertito,
      // come richiesto dal nuovo endpoint timerange che scandisce HBase per chiave numerica
      distinctDates.foreach { epochSecondUTC =>
        val dailyOutput = output.filter(col(dateColumnName) === epochSecondUTC)
        val jsonData = concatenateRowsToJson(dailyOutput, dateColumnName)
        val invertedTimeSuffix = f"${Long.MaxValue - epochSecondUTC}%019d"
        val rowKey = projectId + "_" + hProjectAlgorithmName + "_" + invertedTimeSuffix
        writeToHBase(rowKey, jsonData, hBaseTable, overwrite)
      }
    }

    // Chiudo connessione spark
    spark.stop()
  }

}
