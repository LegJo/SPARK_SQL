import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.SQLException
import  org.apache.spark.sql.AnalysisException

import java.io.{FileOutputStream, PrintStream}
import org.apache.commons.lang3.exception.ExceptionUtils

import SparkSQLApp.{sparkSession}
import configSQLServer.{jdbcUrl, connectionProperties}

object utils {
  /**
   * Gère les exceptions et les imprime avec des informations contextuelles.
   *
   * @param function la fonction dont on veux gerer les exception
   * @param default valeur par defaut qui sera retournés si il y a une exception
   */
  def handleException[T](function: => T, default: T): T = {
    try {
      function
    } catch {
      case ex: Exception => {
        var errMsg:String = getErrorMessage(ex, currentFunctionName)
        printColoredText("red", errMsg)
        var datetime:String = getCurrentDateTimeStr()
        printLine();
        var logLine:String = "> " + datetime + " : " + errMsg
        var line:String = "-".repeat(logLine.length())
        writeInLogFile(line)
        writeInLogFile(logLine)
        writeInLogFile(line)
        writeInLogFile(ExceptionUtils.getStackTrace(ex))
        writeInLogFile(line)
        default
      }
    }
  }

   def getErrorMessage(ex: Exception, functionName:String): String = {
    ex match {
      case sqlEx: SQLException =>
        s"SQL ERROR : Erreur SQL lors de $functionName : ${sqlEx.getMessage}"
      case analysisEx: AnalysisException =>
        s"ANALYSIS ERROR : Erreur d'Analyse lors de $functionName : ${analysisEx.getMessage}"
      case _ =>
        s"ERROR : Une erreur inattendue s'est produite lors de $functionName : ${ex.getMessage}"
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
    val currentMethodName = Thread.currentThread.getStackTrace()(3).getMethodName
    currentMethodName
  }

  private def writeInLogFile(message: String): Unit = {
    val logFilePath = "./src/data/AppLogs.log"
    val fileStream = new FileOutputStream(logFilePath, true)
    val printStream = new PrintStream(fileStream)
    printStream.println(message)
    printStream.close()
  }

  def getCurrentDateTimeStr():String = {
    val formattedDateTime = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    formattedDateTime
  }

  def getColorCode(color: String): String = color.toLowerCase match {
    case "black" => "30m"
    case "red" => "31m"
    case "green" => "32m"
    case "yellow" => "33m"
    case "blue" => "34m"
    case "magenta" => "35m"
    case "cyan" => "36m"
    case "white" => "37m"
    case _ => "39m" // Par défaut, la couleur par défaut est utilisée
  }

  def printColoredText(color: String, msg: String): Unit = {
    val colorCode = getColorCode(color)
    val coloredMsg = s"\u001b[$colorCode$msg\u001b[0m"
    println(coloredMsg)
  }
}
