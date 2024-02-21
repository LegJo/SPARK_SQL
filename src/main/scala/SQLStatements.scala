import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}

import utils._
import DataFrameFunctions._
import SparkSQLApp.{sparkSession}
import configSQLServer.{jdbcUrl, connectionProperties} 

object SQLStatements {
  /**
   * Selectionne des donnees depuis une table dans la base de donnees.
   *
   * @param tableName le nom de la table a partir de laquelle selectionner les donnees
   * @param filter le filtre a appliquer lors de la selection des donnees (facultatif)
   * @param columns les columns de la table a selectionner separé par des , 
   * @return une option de DataFrame contenant les donnees selectionnees, ou None en cas d'erreur
   */
  def selectTable(tableName: String, columns:String = "", filter: String = ""): Option[DataFrame] = {
    handleException[Option[DataFrame]]({
      val df: DataFrame = {
        if (!columns.nonEmpty) {
          if (!filter.nonEmpty) {
            sparkSession.read.jdbc(jdbcUrl, tableName, connectionProperties)
          } else {
            sparkSession.read.jdbc(jdbcUrl, s"(SELECT * FROM $tableName WHERE $filter) tmp", connectionProperties)
          }
        } else {
          if (!filter.nonEmpty) {
            sparkSession.read.jdbc(jdbcUrl, s"(SELECT $columns FROM $tableName) tmp", connectionProperties)
          } else {
            sparkSession.read.jdbc(jdbcUrl, s"(SELECT $columns FROM $tableName WHERE $filter) tmp", connectionProperties)
          }
        }
      }
      Some(df)
    },None)
  }

  /**
   * Insère des donnees dans une table de la base de donnees.
   *
   * @param tableName le nom de la table dans laquelle inserer les donnees
   * @param data le DataFrame contenant les donnees a inserer
   */
  def insertInTable(tableName: String, data: DataFrame): Unit = {
    handleException[Unit]({
      data.write.mode("append").jdbc(jdbcUrl, tableName, connectionProperties)
    },())
  }

  /**
   * Cree une nouvelle table dans la base de donnees.
   *
   * @param tableName le nom de la table a creer
   * @param df le dataframe contennant le schema et le contenue de la table
   */
  def createTable(tableName: String, df: DataFrame): Unit = {
    handleException[Unit]({
      df.write.jdbc(jdbcUrl, tableName, connectionProperties)
    },())
  }

  /**
   * Cree une nouvelle table dans la base de donnees mais overwrite la table si elle existe déjà.
   *
   * @param tableName le nom de la table a creer
   * @param schema le schema de la table a creer
   */
  def overwriteTable(tableName: String, df: DataFrame): Unit = {
    handleException[Unit]({
      df.write.mode("overwrite").jdbc(jdbcUrl, tableName, connectionProperties)
    },())
  }

  /**
   * Vide une table de la base de donnees en supprimant toutes ses donnees.
   *
   * @param tableName le nom de la table a vider
   */
  def truncateTable(tableName: String): Unit = {
    handleException[Unit]({
      var df: DataFrame = getDFFromOptionDF(selectTable(tableName))
      truncate(df)
      overwriteTable(tableName, df)
    }, ())
  }


  /**
   * Met a jour des donnees dans une table de la base de donnees.
   *
   * @param tableName le nom de la table dans laquelle mettre a jour les donnees
   * @param filter le filtre a appliquer lors de la mise a jour des donnees
   * @param data le DataFrame contenant les donnees mises a jour
   */
  def update(tableName: String, filter: String, data: DataFrame): Unit = {
    
  }

  /**
   * Supprime des donnees d'une table de la base de donnees.
   *
   * @param tableName le nom de la table dans laquelle supprimer les donnees
   * @param filter le filtre a appliquer lors de la suppression des donnees
   */
  def delete(tableName: String, filter: String): Unit = {
    
  }
}
