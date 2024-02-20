import java.util.Properties
import java.sql.SQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode

// !!!!!! To run sbt server with jdk11 : sbt -java-home "C:\Program Files\Java\jdk-11"  !!!!! 

object configSQLServer {
  private val jdbcHostname:String = "localhost";
  private val jdbcPort:Int = 1521; 
  private val jdbcSID:String = "xe"; // SID de la base de données Oracle
  private val jdbcUsername:String = "system";
  private val jdbcPassword:String = "dbadmin";
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
    println(s"jdbcUrl: ${jdbcUrl}");
    println(s"Connection: ${connectionProperties}");

    // Récupérer le DataFrame depuis la base de données
    val selectedData: Option[DataFrame] = SQLStatements.select("system.film")
    val selectedDataFromJSON: Option[DataFrame] = utils.loadJSON("C:\\Users\\cleme\\Desktop\\dev\\Scala\\PROJET_COURS\\SPARK_SQL-main\\src\\data\\data.json")


    // // Filtrer les données si elles sont présentes, sinon renvoyer une DataFrame vide
    val DF:DataFrame = DataFrameFunctions.getDFFromOptionDF(selectedDataFromJSON)
    DF.show()

    val filteredDataFrame:DataFrame = DataFrameFunctions.filter(DF, "ANNEE > 1999")
    //filteredDataFrame.show()


    val orderedDataFrame: DataFrame = DataFrameFunctions.orderby(DF, "ANNEE")
    //orderedDataFrame.show()

    val selectedDataFrame: DataFrame = DataFrameFunctions.select(DF, "ANNEE", "TITRE")
    //selectedDataFrame.show()
  
    val groupedDF = DataFrameFunctions.groupby(DF, "GENRE", ("ANNEE", "avg"), ("TITRE", "count"))
    //groupedDF.show()

    val droppedDataFrame: DataFrame = DataFrameFunctions.drop(DF, "ANNEE")
    //droppedDataFrame.show()   

    val distinctDataFrame: DataFrame = DataFrameFunctions.distinct(DF)
    //distinctDataFrame.show() 

    val unionDataFrame: DataFrame = DataFrameFunctions.union(DF, DF)
    //unionDataFrame.show() 

    val limitedDataFrame: DataFrame = DataFrameFunctions.limit(DF, 2)
    //limitedDataFrame.show() 

    val colStatsDataFrame: DataFrame = DataFrameFunctions.colStats(DF)
    //colStatsDataFrame.show() 

    val deleteRowDataFrame: DataFrame = DataFrameFunctions.deleteRows(DF,"ANNEE = 2002")
    //deleteRowDataFrame.show() 

    println("Stopping Spark session...")
    sparkSession.stop()
  }
}
