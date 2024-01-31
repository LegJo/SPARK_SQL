import java.util.Properties
import java.sql.SQLException
import org.apache.spark.sql.{DataFrame, SparkSession}

import utils._
import SparkSQLApp.{sparkSession}
import configSQLServer.{jdbcUrl, connectionProperties} 

object SQLStatements {
  /**
   * Gère les exceptions et les imprime avec des informations contextuelles.
   *
   * @param operation l'operation qui a provoque l'exception
   * @param tableName le nom de la table sur laquelle l'operation a ete effectuee
   * @param ex l'exception levee
   */
  private def handleException(operation: String, tableName: String, ex: Exception): Unit = {
    ex match {
      case sqlEx: SQLException =>
        println(s"SQL ERROR : Erreur SQL lors de $operation sur la table $tableName : ${sqlEx.getMessage}")
      case _ =>
        println(s"ERROR : Une erreur inattendue s'est produite lors de $operation sur la table $tableName : ${ex.getMessage}")
    }
    printLine();
  }

  /**
   * Selectionne des donnees depuis une table dans la base de donnees.
   *
   * @param tableName le nom de la table a partir de laquelle selectionner les donnees
   * @param filter le filtre a appliquer lors de la selection des donnees (facultatif)
   * @return une option de DataFrame contenant les donnees selectionnees, ou None en cas d'erreur
   */
  def select(tableName: String, filter: String = ""): Option[DataFrame] = {
    try {
      val df = if (filter.nonEmpty) {
        sparkSession.read.jdbc(jdbcUrl, s"(SELECT * FROM $tableName WHERE $filter) tmp", connectionProperties)
      } else {
        sparkSession.read.jdbc(jdbcUrl, tableName, connectionProperties)
      }
      Some(df)
    } catch {
      case ex: Exception =>
        handleException("la selection des donnees depuis", tableName, ex)
        None
    }
  }

  /**
   * Insère des donnees dans une table de la base de donnees.
   *
   * @param tableName le nom de la table dans laquelle inserer les donnees
   * @param data le DataFrame contenant les donnees a inserer
   */
  def insert(tableName: String, data: DataFrame): Unit = {
    try {
      data.write.jdbc(jdbcUrl, tableName, connectionProperties)
    } catch {
      case ex: Exception =>
        handleException("l'insertion des donnees dans", tableName, ex)
    }
  }

  /**
   * Met a jour des donnees dans une table de la base de donnees.
   *
   * @param tableName le nom de la table dans laquelle mettre a jour les donnees
   * @param filter le filtre a appliquer lors de la mise a jour des donnees
   * @param data le DataFrame contenant les donnees mises a jour
   */
  def update(tableName: String, filter: String, data: DataFrame): Unit = {
    try {
      data.write.mode("overwrite").jdbc(jdbcUrl, tableName, connectionProperties)
    } catch {
      case ex: Exception =>
        handleException("la mise a jour des donnees dans", tableName, ex)
    }
  }

  /**
   * Supprime des donnees d'une table de la base de donnees.
   *
   * @param tableName le nom de la table dans laquelle supprimer les donnees
   * @param filter le filtre a appliquer lors de la suppression des donnees
   */
  def delete(tableName: String, filter: String): Unit = {
    try {
      val df = sparkSession.read.jdbc(jdbcUrl, tableName, connectionProperties).filter(filter)
      df.write.mode("overwrite").jdbc(jdbcUrl, tableName, connectionProperties)
    } catch {
      case ex: Exception =>
        handleException("la suppression des donnees dans", tableName, ex)
    }
  }

  def executeStatement(statementStr:String): Unit = {
    
  }

  /**
   * Cree une nouvelle table dans la base de donnees.
   *
   * @param tableName le nom de la table a creer
   * @param schema le schema de la table a creer
   */
  def createTable(tableName: String, schema: String): Unit = {
    try {
      sparkSession.sql(s"CREATE TABLE IF NOT EXISTS $tableName ($schema) USING jdbc OPTIONS (url '$jdbcUrl', dbtable '$tableName')")
    } catch {
      case ex: Exception =>
        handleException("la creation de la table", tableName, ex)
    }
  }

  /**
   * Supprime une table de la base de donnees.
   *
   * @param tableName le nom de la table a supprimer
   */
  def dropTable(tableName: String): Unit = {
    try {
      sparkSession.sql(s"DROP TABLE IF EXISTS $tableName")
    } catch {
      case ex: Exception =>
        handleException("la suppression de la table", tableName, ex)
    }
  }

  /**
   * Vide une table de la base de donnees en supprimant toutes ses donnees.
   *
   * @param tableName le nom de la table a vider
   */
  def truncateTable(tableName: String): Unit = {
    try {
      sparkSession.sql(s"TRUNCATE TABLE $tableName")
    } catch {
      case ex: Exception =>
        handleException("la suppression des donnees de la table", tableName, ex)
    }
  }
}
