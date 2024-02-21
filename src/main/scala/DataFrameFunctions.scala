import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column

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
  // addColumn : Ajoute une colonne au DataFrame                                                                   *
  // createDataFrameFromJson : Crée un DataFrame à partir d'un fichier JSON                                        *
  // createDataFrameFromSeq : Crée un DataFrame à partir d'une séquence de maps                                    *
  //                                                                                                               *
  // ***************************************************************************************************************

  

  def emptyDF: DataFrame = {
    sparkSession.emptyDataFrame
  }

  // Recuperer Dataframe depuis un Option[DataFrame]
  def getDFFromOptionDF(OptionDF: Option[DataFrame]): DataFrame = {
    OptionDF.getOrElse(emptyDF)
  }

  def filter(df: DataFrame, conditions: String): DataFrame = {
    handleException[DataFrame]({
      df.filter(conditions)
    },emptyDF)
  }

  def orderby(df: DataFrame, columns: String*): DataFrame = {
    handleException[DataFrame]({
      df.orderBy(columns.map(c => col(c)): _*)
    },emptyDF)
  }

  def groupby(dataFrame: DataFrame, columns: String, aggregations: (String, String)*): DataFrame = {
    handleException[DataFrame]({
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

      aggregatedDF
    },emptyDF)
  }

  def select(dataFrame: DataFrame, columns: String*): DataFrame = {
    handleException[DataFrame]({
      dataFrame.select(columns.map(col): _*)
    },emptyDF)
  }

  def drop(dataFrame: DataFrame, columns: String*): DataFrame = {
    handleException[DataFrame]({
      dataFrame.drop(columns: _*)
    },emptyDF)
  }

  def distinct(dataFrame: DataFrame): DataFrame = {
    handleException[DataFrame]({
      dataFrame.distinct()
    },emptyDF)
  }

  def union(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
    handleException[DataFrame]({
      dataFrame1.union(dataFrame2)
    },emptyDF)
  }

  def limit(dataFrame: DataFrame, numRows: Int): DataFrame = {
    handleException[DataFrame]({
      dataFrame.limit(numRows)
    },emptyDF)
  }

  def colStats(dataFrame: DataFrame): DataFrame = {
    handleException[DataFrame]({
      dataFrame.describe()
    },emptyDF)
  }

  def deleteRows(dataFrame: DataFrame, filterExpr: String): DataFrame = {
    handleException[DataFrame]({
      filter(dataFrame, s"NOT ($filterExpr)")
    },dataFrame)
  }

  def truncate(df: DataFrame): DataFrame = {
    handleException[DataFrame]({
      df.filter("1=0")
    },df)
  }

  def leftJoin(df1: DataFrame, df2: DataFrame, joinColumn: String): DataFrame = {
    handleException[DataFrame]({
      df1.join(df2, joinColumn, "left")
    },emptyDF)
  }

  def rightJoin(df1: DataFrame, df2: DataFrame, joinColumn: String): DataFrame = {
    handleException[DataFrame]({
      df1.join(df2, joinColumn, "right")
    },emptyDF)
  }

  def innerJoin(df1: DataFrame, df2: DataFrame, joinColumn: String): DataFrame = {
    handleException[DataFrame]({
      df1.join(df2, joinColumn, "inner") // pas necessaire de préciser "inner" c'est par defaut
    },emptyDF)
  }

  def outerJoin(df1: DataFrame, df2: DataFrame,joinColumn: String): DataFrame = {
    handleException[DataFrame]({
      df1.join(df2, joinColumn, "outer")
    },emptyDF)
  }

  def renameColumn(df: DataFrame, oldColumnName: String, newColumnName: String): DataFrame = {
    handleException[DataFrame]({
      df.withColumnRenamed(oldColumnName, newColumnName)
    },df)
  }

  def convertColumn(df: DataFrame, columnName: String, newType: DataType): DataFrame = {
    handleException[DataFrame]({
      df.withColumn(columnName, col(columnName).cast(newType))
    },df)
  }

  // Fonction qui permet de faire des Aggregation de column
  def computeAggregate(df: DataFrame, columnName: String, operation: String): DataFrame = {
    val aggOperation = operation.toLowerCase match {
      case "sum"     => sum(col(columnName))
      case "count"   => count(col(columnName))
      case "max"     => max(col(columnName))
      case "min"     => min(col(columnName))
      case "average" => avg(col(columnName))
      case _ =>
        throw new IllegalArgumentException("Opération non prise en charge")
    }
    handleException[DataFrame]({
       df.agg(aggOperation.as(operation))
    },df)   
  }

   // Fonction pour ajouter une colonne au DataFrame
  def addColumn(dataFrame: DataFrame, columnName: String, columnType: DataType, values: Seq[Any]): DataFrame = {
    handleException[DataFrame]({
        val litValues = values.map(lit(_))
        val newColumn = litValues.zipWithIndex.foldLeft(dataFrame)((accDF, valueWithIndex) => {
        val (value, index) = valueWithIndex
        accDF.withColumn(columnName + index, lit(value).cast(columnType))
        })
        newColumn
    }, dataFrame)
  }


  // Fonction pour créer un DataFrame à partir d'un fichier JSON
  def createDataFrameFromJson(jsonPath: String): Option[DataFrame] = {
    handleException[Option[DataFrame]]({
      Some(sparkSession.read.option("mode", "DROPMALFORMED").json(jsonPath))
    }, None)
  }

  // Fonction pour créer un DataFrame à partir d'une séquence de maps
  def createDataFrameFromSeq(seq: Seq[Product], schema: StructType): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val rdd = spark.sparkContext.parallelize(seq.map(Row.fromTuple))
    spark.createDataFrame(rdd, schema)
  }
}
