package it.acsoftware.hyperiot.spark.countby

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

object CountBy {

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
      .appName( "CountBy")
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

    // get first HPacketField ID
    //val hPacketFieldId = root.input.each.mappedInputList.each.packetFieldId.long.getAll(jobConfig).headOption.get

    // get first HPacketField type
    //var hPacketFieldType =
    //  root.input.each.mappedInputList.each.algorithmInput.fieldType.string.getAll(jobConfig).headOption.get.toLowerCase()
    // one of input type can be "number". However, SparkSQL cannot cast to number, but it does to decimal
    //hPacketFieldType = if (hPacketFieldType == "number") "decimal" else hPacketFieldType
    
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

    /*
      NEW STRUCTURE -> the name of the column are the ID OF THE FIELD, with every value
      +------------------+------------------+
      |               258|               259|
      +------------------+------------------+
      |17.065334266630934|24.387431486170836|
      | 17.49115885108567| 20.61515043824036|
      |17.548476059520798|22.074255567795834|
      | 17.25160723622318|20.249845335735195|
      |17.574048740041682| 23.78246424723059|
      | 16.16587997987633|21.722289461745277|
      |17.402357500018496|23.120702018829107|
      |17.716385295900697| 20.52234361849609|
      |17.026509669271334|21.275516666433745|
      |16.220651528462085|21.743694535667622|
      | 17.03040295399606|  21.1043929342866|
      |17.811436267536784| 21.35966792297221|
      |17.963167367456514|22.233382185749427|
      |16.680287535392093| 22.70999556169612|
      |16.628682242312884| 24.15958051612948|
      | 16.09687925614665| 24.29914295424012|
      |16.215968523546593| 23.93743502206239|
      |17.189700655002948|21.382208155103687|
      |16.105960813121243|20.703543454967217|
      |16.865080230870337|22.698428846737595|
      +------------------+------------------+
      Goal: get value of HPacketField.
      Why? HPacketField has value with a type among the following ones: INTEGER, LONG, FLOAT,
      DOUBLE, BOOLEAN, STRING. In avro, we reach this via union type, which SparkSQL decode as struct.

      root
      |-- map_values(fields): array (nullable = true)
      |    |-- element: struct (containsNull = true)
      |    |    |-- name: string (nullable = true)
      |    |    |-- description: string (nullable = true)
      |    |    |-- type: string (nullable = true)
      |    |    |-- multiplicity: string (nullable = true)
      |    |    |-- packet: long (nullable = true)
      |    |    |-- value: struct (nullable = true)
      |    |    |    |-- member0: integer (nullable = true)
      |    |    |    |-- member1: long (nullable = true)
      |    |    |    |-- member2: float (nullable = true)
      |    |    |    |-- member3: double (nullable = true)
      |    |    |    |-- member4: boolean (nullable = true)
      |    |    |    |-- member5: string (nullable = true)
      |    |    |-- id: long (nullable = true)
      |    |    |-- categoryIds: array (nullable = true)
      |    |    |    |-- element: long (containsNull = true)
      |    |    |-- tagIds: array (nullable = true)
      |    |    |    |-- element: long (containsNull = true)

      (It is the schema of fields property of HPacket)


      This struct has keys with prefix "member", i.e. member0, member1, member2, member3, member4 and member5.
      Each key has a value associated to it of a specific type ( , long, float, double, boolean, string).
      One of these value is not equal to null at most. See the example:

      SCALAR SCHEMA:
      +------+-------+-------+-------+------------------+-------+-------+
      |type  |member0|member1|member2|member3           |member4|member5|
      +------+-------+-------+-------+------------------+-------+-------+
      |DOUBLE|null   |null   |null   |43.59878036402215 |null   |null   |
      |DOUBLE|null   |null   |null   |42.45798705747769 |null   |null   |
      |DOUBLE|null   |null   |null   |44.908155679735856|null   |null   |
      +------+-------+-------+-------+------------------+-------+-------+

      During the lifecycle of HPacketField, its value type could be change. It is mapped to an input, which is chosen by
      user while he configures the algorithm. This input must have its own type, to avoid different conversions
      if algorithm exploits type inside HPacketField. For example: mean algorithm works on double types;
      HPacketField had double type as original type, but after a while it changed to float. Input type of algorithm
      configuration ensures that all values are double.

      The proposal: apply coalesce to get the unique not null value.
      Coalesce works on elements of the same type, cast them as string.
      Get back the original type of selected value through the type of configuration input

    */

    val output = values
      .groupBy(hPacketFieldIds.map(id => col(id.toString)): _*)
      .count()
      .withColumnRenamed("count", outputName)
      .withColumn("timestamp", current_timestamp().cast("long"))

    println("RESULT")
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
