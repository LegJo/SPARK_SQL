import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.SQLException
import java.io.{FileOutputStream, PrintStream}

import SparkSQLApp.{sparkSession}
import configSQLServer.{jdbcUrl, connectionProperties}

object utils {
  /**
   * Gère les exceptions et les imprime avec des informations contextuelles.
   *
   * @param function la fonction dont on veux gerer les exception
   * @param default l'exception levee
   */ 
  def handleException[T](function: => T, default: T): T = {
    try {
      function
    } catch {
      case ex: Exception => {
        var errMsg:String = "\u001b[31m"
        ex match {
          case sqlEx: SQLException =>
            errMsg += s"SQL ERROR : Erreur SQL lors de $function : ${sqlEx.getMessage}" + "\u001b[0m"
          case analysisEx: org.apache.spark.sql.AnalysisException =>
            errMsg += s"ANALYSIS ERROR : Erreur d'Analyse lors de $function : ${analysisEx.getMessage}" + "\u001b[0m"
          case _ =>
            errMsg += s"ERROR : Une erreur inattendue s'est produite lors de $function : ${ex.getMessage}" + "\u001b[0m"
        }
        println(errMsg)
        writeInLogFile(errMsg)
        printLine();
        default
      }
    }
  }

  def printLine(): Unit = {
    println("=" * 60);
  }

  def printDataFrame(df: DataFrame): Unit = {
      df.show();
      printLine();    
  }
  
  def currentFunctionName: String = {
    val currentMethodName = Thread.currentThread.getStackTrace()(2).getMethodName
    currentMethodName
  }

  private def writeInLogFile(message: String): Unit = {
    val logFilePath = "./src/data/AppLogs.log"
    val fileStream = new FileOutputStream(logFilePath, true)
    val printStream = new PrintStream(fileStream)
    printStream.println(message)
    printStream.close()
  }
}
