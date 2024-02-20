import org.apache.spark.sql.{DataFrame, SparkSession}
import SparkSQLApp.{sparkSession}
import configSQLServer.{jdbcUrl, connectionProperties}

object utils {
  def printLine(): Unit = {
    println("=" * 60);
  }

  def printDataFrame(df: Option[DataFrame]): Unit = {
    df match {
      case None =>
        println("None value was passed as a DataFrame in printDataFrame");
      case _ => {
        df.foreach(dataRow => dataRow.show());
        printLine();
      }
    }
  }

    def loadJSON(filePath: String): Option[DataFrame] = {
        try {
            val jsonDF = sparkSession.read.option("mode", "DROPMALFORMED").json(filePath)
            Some(jsonDF)
        } catch {
            case e: Exception =>
            println(s"An error occurred while loading JSON file: ${e.getMessage}")
            None
        }
    }
}
