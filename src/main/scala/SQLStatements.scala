import java.util.Properties
import org.apache.spark.sql.{SparkSession, DataFrame, Column, Row, SaveMode}

import utils._
import DataFrameFunctions._
import SparkSQLApp.{sparkSession}
import configSQLServer.{jdbcUrl, connectionProperties, connection, schema}
import java.sql.{DriverManager, Connection, Statement, ResultSet}

/**
 * @brief Object contenant les fonctions pour exécuter des requêtes SQL sur une base de données externe via JDBC et Spark SQL.
 */
object SQLStatements {

  /**
   * @brief Exécute une instruction SQL qui update la database (INSERT, UPDATE, DELETE, DROP).
   * Pour pallier les limitation de SparkSQL : utilise Java SQL et non pas SparkSQL
   *
   * @param statement l'instruction SQL à exécuter
   * @return le DataFrame résultant de l'exécution de l'instruction SQL
   */
  def executeUpdateStatement(statement:String):Int = {
    handleException[Int]({
      var stmt:Statement = connection.createStatement();
      val sql: String = s"${statement}";
      stmt.executeUpdate(sql);
    },-1)
  }

  /**
   * @brief Exécute une instruction SQL qui retourne des données et les retourne dans DataFrame en résultat.
   * Pour pallier les limitation de SparkSQL : utilise Java SQL et non pas SparkSQL
   *
   * @param statement l'instruction SQL à exécuter
   * @return le DataFrame résultant de l'exécution de l'instruction SQL
   */
  def executeQueryStatement(statement:String):DataFrame = {
    handleException[DataFrame]({
      var stmt:Statement = connection.createStatement();
      val sql: String = s"${statement}";
      val resSet:ResultSet = stmt.executeQuery(sql);
      createDataFrameFromResultSet(resSet)
    },emptyDF)
  }

  /**
   * @brief Sélectionne des données depuis une table dans la base de données.
   *
   * @param tableName le nom de la table à partir de laquelle sélectionner les données
   * @param columns les colonnes de la table à sélectionner séparées par des virgules
   * @param filter le filtre à appliquer lors de la sélection des données (facultatif)
   * @return une option de DataFrame contenant les données sélectionnées, ou None en cas d'erreur
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
   * @brief Insère des données dans une table de la base de données.
   *
   * @param tableName le nom de la table dans laquelle insérer les données
   * @param data le DataFrame contenant les données à insérer
   */
  def insertInTable(tableName: String, data: DataFrame): Unit = {
    handleException[Unit]({
      data.write.mode("append").jdbc(jdbcUrl, tableName, connectionProperties)
    },())
  }

  /**
   * @brief Crée une nouvelle table dans la base de données.
   *
   * @param tableName le nom de la table à créer
   * @param df le dataframe contenant le schéma et le contenu de la table
   */
  def createTable(tableName: String, df: DataFrame): Unit = {
    handleException[Unit]({
      df.write.jdbc(jdbcUrl, tableName, connectionProperties)
    },())
  }

  /**
   * @brief Supprime une table de la base de données.
   *
   * @param tableName le nom de la table à supprimer
   */
  def dropTable(tableName:String):Unit = {
    handleException[Unit]({
      executeUpdateStatement(s"DROP TABLE ${tableName} ");
    },())
  }

  /**
   * @brief Crée une nouvelle table dans la base de données mais remplace la table existante si elle existe déjà.
   *
   * @param tableName le nom de la table à créer
   * @param df le DataFrame de la table qui remplacera l'ancienne
   */
  def overwriteTable(tableName: String, df: DataFrame): Unit = {
    handleException[Unit]({
      if(tableExists(tableName)) {
        dropTable(tableName)
      }
      createTable(tableName,df)
    },())
  }

  /**
   * @brief Vide une table de la base de données en supprimant toutes ses données.
   * En utilisant les fonctions DataFrame.
   *
   * @param tableName le nom de la table à vider
   */
  def truncateTableUsingDF(tableName: String): Unit = {
    handleException[Unit]({
      var df: DataFrame = getDFFromOptionDF(selectTable(tableName))
      truncate(df)
      overwriteTable(tableName, df)
    }, ())
  }


  /**
   * @brief Vide une table de la base de données en supprimant toutes ses données.
   *
   * @param tableName le nom de la table à vider
   */
  def truncateTable(tableName:String):Unit = {
    handleException[Unit]({
      executeUpdateStatement(s"TRUNCATE TABLE ${tableName} ");
      println(s"Table ${tableName} dropped...")
    },())
  }

  /**
   * @brief Met à jour des données dans une table de la base de données.
   *
   * @param tableName le nom de la table dans laquelle mettre à jour les données
   * @param filter Column Filtre du DataFrame permettant de filtrer ce qui sera mis à jour (par exemple : col("id") === 2)
   * @param columnName nom de la colonne à mettre à jour
   * @param newValue la nouvelle valeur de la colonne
   */
  def update(tableName: String, filter: Column, columnName:String, newValue:Any): Unit = {
    handleException[Unit]({
      var df:DataFrame = getDFFromOptionDF(selectTable(tableName))
      df = updateRows(df, filter, columnName, newValue)
      overwriteTable(tableName, df)
    },())
  }

  /**
   * @brief Supprime des données d'une table de la base de données.
   *
   * @param tableName le nom de la table dans laquelle supprimer les données
   * @param filter le filtre à appliquer lors de la suppression des données
   */
  def delete(tableName: String, filter: String): Unit = {
    handleException[Unit]({
      var df:DataFrame = getDFFromOptionDF(selectTable(tableName))
      df = deleteRows(df, filter)
      overwriteTable(tableName, df)
    },())
  }

  /**
   * @brief Copie les données d'une table existante dans une nouvelle table avec un nom spécifié.
   *
   * @param tableName le nom de la table source à copier
   * @param copyName le nom de la nouvelle table de destination
   */
  def copyTable(tableName:String, copyName:String):Unit = {
    handleException[Unit]({
      var df:DataFrame = getDFFromOptionDF(selectTable(tableName))
      createTable(copyName, df)
    },())
  }

  /**
   * @brief Copie les données d'une table existante dans une nouvelle table avec un nom spécifié, remplaçant toute table existante avec le même nom.
   *
   * @param tableName le nom de la table source à copier
   * @param copyName le nom de la nouvelle table de destination
   */
  def copyTableForced(tableName:String, copyName:String):Unit = {
    handleException[Unit]({
      var df:DataFrame = getDFFromOptionDF(selectTable(tableName))
      overwriteTable(copyName, df)
    },())
  }

  /**
   * @brief Exécute une Stored Procedure avec des paramètres et renvoie le résultat dans un DataFrame.
   *
   * @param procedureName le nom de la Stored Procedure à exécuter
   * @param params les paramètres de la Stored Procedure
   * @return le DataFrame résultant de l'exécution de la Stored Procedure
   *
   * @callExemple executeStoredProcedure(procedureName, Seq(param1, param2))
   */
  def executeStoredProcedure(procedureName: String, params: Seq[Any]): DataFrame = {
    val paramStr = params.mkString(", ")
    val statement = s"EXEC $procedureName $paramStr"
    executeQueryStatement(statement)
  }

  /**
   * @brief verifie si la table existe en database
   *
   * @param tableName le nom de la table 
   */
  def tableExists(tableName: String): Boolean = {
    tableName.replaceAll(s".$schema", "")
    val metaData = connection.getMetaData
    val rs = metaData.getTables(null, null, "%", Array("TABLE"))
    var exists = false
    while (rs.next() && !exists) {
      val existingTableName = rs.getString("TABLE_NAME")
      if (existingTableName.equalsIgnoreCase(tableName)) {
        exists = true
      }
    }
    rs.close()
    exists
  }

  def listTables(): Unit = {
    val metaData = connection.getMetaData
    val rs = metaData.getTables(null, null, "%", Array("TABLE"))
    while (rs.next()) {
      val tableName = rs.getString("TABLE_NAME")
      println(tableName)
    }
    rs.close()
  }

  /**
   * @brief Appelle une procédure stockee dans la base de donnees.
   *
   * @param procedureName le nom de la procédure stockée
   * @param parameters les paramètres à passer à la procédure
   */
  def callStoredProcedure(procedureName: String, parameters: Option[String]*): Unit = {
    handleException[Unit]({
      val connection = java.sql.DriverManager.getConnection(jdbcUrl, connectionProperties)
      try {
        val callableStatement = connection.prepareCall(s"{call $procedureName(${parameters.flatten.mkString(", ")})}")
        callableStatement.execute()
      } finally {
        connection.close()
      }
    }, ())
  }

}
