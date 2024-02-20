import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utils._
import SparkSQLApp.{sparkSession}
import configSQLServer.{jdbcUrl, connectionProperties}

object DataFrameFunctions {

  // **** Liste de toutes les fonctions ****************************************************************************
  //                                                                                                               *
  // emptyDF : Crée un DataFrame vide                                                                              *
  // getDFFromOptionDF : Récupère un DataFrame à partir d'un Option[DataFrame]                                     *
  // checkDataFrameAndExecute : Vérifie si le DataFrame n'est pas vide avant d'exécuter une action                 *
  // filter : Filtrer les données selon une condition sur une colonne donnée                                       *
  // orderBy : Trie les lignes du DataFrame en fonction des colonnes spécifiées                                    *
  // groupBy : Regroupe les lignes du DataFrame en fonction des valeurs des colonnes spécifiées                    *
  // select : Sélectionne les colonnes spécifiées pour chaque ligne du DataFrame                                   *
  // drop : Supprime les colonnes spécifiées du DataFrame                                                          *
  // distinct : Retourne un nouveau DataFrame avec des lignes distinctes                                           *
  // union : Effectue l'union avec un autre DataFrame, ajoutant toutes les lignes                                  *
  // limit : Limite le DataFrame aux premières 'n' lignes                                                          *
  // colStats : Calculer des statistiques sur les colonnes numériques                                              *
  //                                                                                                               *
  // ***************************************************************************************************************

  def emptyDF(): DataFrame = {
    sparkSession.emptyDataFrame
  }

  // Recuperer Dataframe depuis un Option[DataFrame]
  def getDFFromOptionDF(OptionDF: Option[DataFrame]): DataFrame = {
    OptionDF.getOrElse(emptyDF())
  }

  def checkDataFrameAndExecute(
      DataFrame: DataFrame,
      action: => DataFrame
  ): DataFrame = {
    if (DataFrame.isEmpty) {
      println("DataFrame is empty. Cannot perform operation.")
      emptyDF() // Renvoyer l'instance actuelle de CustomDataFrame
    } else {
      action
    }
  }

  def filter(DataFrame: DataFrame, conditions: String): DataFrame = {
    checkDataFrameAndExecute(DataFrame, DataFrame.filter(conditions))
  }

  def orderby(dataFrame: DataFrame, columns: String*): DataFrame = {
    checkDataFrameAndExecute(
      dataFrame,
      dataFrame.orderBy(columns.map(c => col(c)): _*)
    )
  }

  def groupby(
      dataFrame: DataFrame,
      columns: String,
      aggregations: (String, String)*
  ): DataFrame = {
    val groupByColumns = columns.split(",")
    val groupedDF =
      dataFrame.groupBy(groupByColumns.head, groupByColumns.tail: _*)

    val aggExpressions = aggregations.map { case (col, aggType) =>
      val aggExpr = aggType.toLowerCase match {
        case "sum"   => sum(col)
        case "avg"   => avg(col)
        case "min"   => min(col)
        case "max"   => max(col)
        case "count" => count(col)
        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported aggregation type: $aggType"
          )
      }
      aggExpr.alias(s"${col}_${aggType}")
    }

    val aggregatedDF =
      groupedDF.agg(aggExpressions.head, aggExpressions.tail: _*)

    checkDataFrameAndExecute(dataFrame, aggregatedDF)
  }

  def select(dataFrame: DataFrame, columns: String*): DataFrame = {
    checkDataFrameAndExecute(dataFrame, dataFrame.select(columns.map(col): _*))
  }

  def drop(dataFrame: DataFrame, columns: String*): DataFrame = {
    checkDataFrameAndExecute(dataFrame, dataFrame.drop(columns: _*))
  }

  def distinct(dataFrame: DataFrame): DataFrame = {
    checkDataFrameAndExecute(dataFrame, dataFrame.distinct())
  }

  def union(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
    checkDataFrameAndExecute(dataFrame1, dataFrame1.union(dataFrame2))
  }

  def limit(dataFrame: DataFrame, numRows: Int): DataFrame = {
    checkDataFrameAndExecute(dataFrame, dataFrame.limit(numRows))
  }

  def colStats(dataFrame: DataFrame): DataFrame = {
    checkDataFrameAndExecute(dataFrame, dataFrame.describe())
  }

  def deleteRows(dataFrame: DataFrame, filterExpr: String): DataFrame = {
    checkDataFrameAndExecute(dataFrame, filter(dataFrame, s"NOT ($filterExpr)"))
  }

  def truncate(df: DataFrame): DataFrame = {
    df.filter("1=0")
  }

  def leftJoin(
      df1: DataFrame,
      df2: DataFrame,
      joinColumn: String
  ): DataFrame = {
    val resultDF = df1.join(df2, joinColumn, "left")
    resultDF
  }

  def rightJoin(
      df1: DataFrame,
      df2: DataFrame,
      joinColumn: String
  ): DataFrame = {
    val resultDF = df1.join(df2, joinColumn, "right")
    resultDF
  }

  def innerJoin(
      df1: DataFrame,
      df2: DataFrame,
      joinColumn: String
  ): DataFrame = {
    val resultDF = df1.join(
      df2,
      joinColumn,
      "inner"
    ) // pas necessaire de préciser "inner" c'est par defaut
    resultDF
  }

  def outerJoin(
      df1: DataFrame,
      df2: DataFrame,
      joinColumn: String
  ): DataFrame = {
    val resultDF = df1.join(df2, joinColumn, "outer")
    resultDF
  }

  def renameColumn(
      df: DataFrame,
      oldColumnName: String,
      newColumnName: String
  ): DataFrame = {
    // Utiliser withColumnRenamed pour renommer la colonne
    val renamedDF = df.withColumnRenamed(oldColumnName, newColumnName)
    renamedDF
  }

  // def convertColumn(df: DataFrame, columnName: String, newType: DataType): DataFrame = {
  //     try {
  //     // Utiliser withColumn et cast pour changer le type de la colonne
  //     val updatedDF = df.withColumn(columnName, col(columnName).cast(newType))
  //     updatedDF
  //     } catch {
  //     case e: Exception =>
  //         // Gérer l'erreur de conversion de type
  //         println(s"Erreur lors de la conversion de la colonne $columnName : ${e.getMessage}")
  //         df // Retourner le DataFrame d'origine en cas d'erreur
  //     }
  // }

  // Fonction qui permet de faire des Aggregation de column
  def computeAggregate(
      df: DataFrame,
      columnName: String,
      operation: String
  ): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val aggOperation = operation.toLowerCase match {
      case "sum"     => sum(col(columnName))
      case "count"   => count(col(columnName))
      case "max"     => max(col(columnName))
      case "min"     => min(col(columnName))
      case "average" => avg(col(columnName))
      case _ =>
        throw new IllegalArgumentException("Opération non prise en charge")
    }

    df.agg(aggOperation.as(operation))
  }

}
