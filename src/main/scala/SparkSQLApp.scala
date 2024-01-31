import java.util.Properties
import java.sql.SQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
//To run sbt server with jdk11 : sbt -java-home "C:\Program Files\Java\jdk-11"


object configSQLServer {
    private val SQLServerDriver:String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    private val jdbcHostname:String = "localhost";
    private val jdbcPort:Int = 1433; 
    private val jdbcDatabase:String = "JL_DB_PROD";
    private val jdbcUsername:String = "jl_admin";
    private val jdbcPassword:String = "dbadmin";

    Class.forName(SQLServerDriver);
    val jdbcUrl:String = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}";
    val connectionProperties:Properties = new Properties() {{
      put("user", jdbcUsername);
      put("password", jdbcPassword);
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
    import utils._
    import sparkSession.implicits._
    import configSQLServer._

    printLine();
    println(s"jdbcUrl: ${jdbcUrl}");
    println(s"Connection: ${connectionProperties}");
    printLine();

    //SQL Statements Exemples 
    var selectedData:Option[DataFrame] = SQLStatements.select("dbo.film");
    printDataFrame(selectedData);
    
    val dataToInsert:DataFrame = Seq((777, "Spark Le Film", "SparkGenre", 2025)).toDF("num_ind", "titre", "genre", "annee");
    SQLStatements.insert("dbo.film", dataToInsert);
    selectedData = SQLStatements.select("dbo.film");
    printDataFrame(selectedData);

    SQLStatements.delete("dbo.film", "num_ind = 777");
    selectedData = SQLStatements.select("dbo.film");
    printDataFrame(selectedData);

    // Arrêter la session Spark
    sparkSession.stop()
  }
}
