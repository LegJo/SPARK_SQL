import java.util.Properties
import java.sql.SQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StructField, StructType, StringType}

// !!!!!! To run sbt server with jdk11 : sbt -java-home "C:\Program Files\Java\jdk-11"  !!!!! 

object configSQLServer {
  private val jdbcHostname:String = "localhost";
  private val jdbcPort:Int = 1522; 
  private val jdbcSID:String = "xe"; // SID de la base de données Oracle
  private val jdbcUsername:String = "system";
  private val jdbcPassword:String = "metTonPassword";
  Class.forName("oracle.jdbc.driver.OracleDriver")
  val jdbcUrl:String = s"jdbc:oracle:thin:@${jdbcHostname}:${jdbcPort}:${jdbcSID}"
  val connectionProperties:Properties = new Properties() {{
    put("user", jdbcUsername)
    put("password", jdbcPassword)
  }};
}


object SparkSQLApp {
  val sparkSession:SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName("SparkSQL")
      .config("spark.log.level", "ERROR")
      .getOrCreate();
  }

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._
    import configSQLServer._
    import utils._
    import DataFrameFunctions._

    println("Creating Spark session...")
    println(s"jdbcUrl: ${jdbcUrl}")
    // println(s"Connection: ${connectionProperties}")

    // Récupérer le DataFrame depuis la base de données
    val selectedData: Option[DataFrame] = SQLStatements.selectTable("system.film")
    val selectedDataFromJSON: Option[DataFrame] = utils.loadJSON(".\\src\\data\\data.json")

    // Filtrer les données si elles sont présentes, sinon renvoyer une DataFrame vide
    val DF:DataFrame = DataFrameFunctions.getDFFromOptionDF(selectedDataFromJSON)
    printDataFrame(DF)

    // val filteredDataFrame:DataFrame = DataFrameFunctions.filter(DF, "ANNEE > 1999")
    // printDataFrame(filteredDataFrame)

    // val orderedDataFrame: DataFrame = DataFrameFunctions.orderby(DF, "ANNEE")
    // printDataFrame(orderedDataFrame)

    // val selectedDataFrame: DataFrame = DataFrameFunctions.select(DF, "ANNEE", "TITRE")
    // printDataFrame(selectedDataFrame)
    
    // val groupedDF = DataFrameFunctions.groupby(DF, "GENRE", ("ANNEE", "avg"), ("TITRE", "count"))
    // printDataFrame(groupedDF)

    // val droppedDataFrame: DataFrame = DataFrameFunctions.drop(DF, "ANNEE")
    // printDataFrame(droppedDataFrame)   

    // val distinctDataFrame: DataFrame = DataFrameFunctions.distinct(DF)
    // printDataFrame(distinctDataFrame) 

    // val unionDataFrame: DataFrame = DataFrameFunctions.union(DF, DF)
    // printDataFrame(unionDataFrame) 

    // val limitedDataFrame: DataFrame = DataFrameFunctions.limit(DF, 2)
    // printDataFrame(limitedDataFrame) 

    // val colStatsDataFrame: DataFrame = DataFrameFunctions.colStats(DF)
    // printDataFrame(colStatsDataFrame) 

    // val deleteRowDataFrame: DataFrame = DataFrameFunctions.deleteRows(DF,"ANNEE = 2002")
    // printDataFrame(deleteRowDataFrame) 

    // Tester la fonction addColumn
    val valuesToAdd = Seq("value1", "value2", "value3")
    val dfWithNewColumn = addColumn(DF, "newColumn", StringType, valuesToAdd)
    printDataFrame(dfWithNewColumn)

    // Tester la fonction createDataFrameFromJson
    val jsonPath = ".\\src\\data\\data.json"
    val jsonDF = createDataFrameFromJson(jsonPath)
    jsonDF.foreach(printDataFrame) // Afficher le DataFrame créé à partir du fichier JSON

    // Tester la fonction createDataFrameFromSeq
    val seqData = Seq(
      Map("col1" -> 1, "col2" -> "A"),
      Map("col1" -> 2, "col2" -> "B"),
      Map("col1" -> 3, "col2" -> "C")
    )
    val seqDF = createDataFrameFromSeq(seqData)
    seqDF.foreach(printDataFrame) // Afficher le DataFrame créé à partir de la séquence de maps

    println("Stopping Spark session...")
    sparkSession.stop()
  }
}
