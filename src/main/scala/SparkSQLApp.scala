import java.util.Properties
import java.sql.SQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
//To run sbt server with jdk11 : sbt -java-home "C:\Users\Adamou\AppData\Local\Coursier\cache\arc\https\github.com\adoptium\temurin11-binaries\releases\download\jdk-11.0.21%252B9\OpenJDK11U-jdk_x64_windows_hotspot_11.0.21_9.zip\jdk-11.0.21+9"


object configSQLServer {
    private val SQLServerDriver:String = "oracle.jdbc.driver.OracleDriver"
    private val jdbcHostname:String = "localhost";
    private val jdbcPort:Int = 1521;
    private val jdbcDatabase:String = "XE";
    private val jdbcUsername:String = "system";
    private val jdbcPassword:String = "root";
    Class.forName(SQLServerDriver);
    val jdbcUrl: String = s"jdbc:oracle:thin:@${jdbcHostname}:${jdbcPort}:${jdbcDatabase}"
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
