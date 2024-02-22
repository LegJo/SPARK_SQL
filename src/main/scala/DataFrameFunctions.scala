import org.apache.spark.sql.{SparkSession, DataFrame, Column, Row, SaveMode}
import org.apache.spark.sql.functions._
<<<<<<< Updated upstream
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column
=======
import org.apache.spark.sql.types._
import java.sql.{DriverManager, Connection, Statement, ResultSet}
import scala.collection.JavaConverters._

>>>>>>> Stashed changes

import utils._
import SparkSQLApp.{sparkSession}
import configSQLServer.{jdbcUrl, connectionProperties}

object DataFrameFunctions {

  // **** Liste de toutes les fonctions ****************************************************************************
  //                                                                                                               *
  // emptyDF : Crée un DataFrame vide                                                                              *
  // getDFFromOptionDF : Récupère un DataFrame à partir d'un Option[DataFrame]                                     *
  // filter : Filtrer les données selon une condition sur une colonne donnée                                       *
  // orderBy : Trie les lignes du DataFrame en fonction des colonnes spécifiées                                    *
  // groupBy : Regroupe les lignes du DataFrame en fonction des valeurs des colonnes spécifiées                    *
  // select : Sélectionne les colonnes spécifiées pour chaque ligne du DataFrame                                   *
  // drop : Supprime les colonnes spécifiées du DataFrame                                                          *
  // distinct : Retourne un nouveau DataFrame avec des lignes distinctes                                           *
  // union : Effectue l'union avec un autre DataFrame, ajoutant toutes les lignes                                  *
  // limit : Limite le DataFrame aux premières 'n' lignes                                                          *
  // colStats : Calculer des statistiques sur les colonnes numériques                                              *
<<<<<<< Updated upstream
  // addColumn : Ajoute une colonne au DataFrame                                                                   *
  // createDataFrameFromJson : Crée un DataFrame à partir d'un fichier JSON                                        *
  // createDataFrameFromSeq : Crée un DataFrame à partir d'une séquence de maps                                    *
=======
  // printSchema : affiche le Schema du dataframe                                                                  *
  // addColumn : Ajoute une colonne au DataFrame                                                                   *
  // createDataFrameFromJson : Crée un DataFrame à partir d'un fichier JSON                                        *
  // createDataFrameFromSeq : Crée un DataFrame à partir d'une séquence de maps                                    *
  // createDataFrameFromResultSet : Fonction pour créer un DataFrame à partir d'un ResultSet                       *
>>>>>>> Stashed changes
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
      val joined = df1.join(df2, joinColumn, "left")
      if (joined.columns.contains(joinColumn)) {
        joined.select(df1.columns.map(col): _*)
      } else {
        df1
      }
    }, df1)
  }


  def rightJoin(df1: DataFrame, df2: DataFrame, joinColumn: String): DataFrame = {
    handleException[DataFrame]({
      val joined = df1.join(df2, joinColumn, "right")
      if (joined.columns.contains(joinColumn)) {
        joined.select(df1.columns.map(col): _*)
      } else {
        df1
      }
    }, df1)
  }

  def innerJoin(df1: DataFrame, df2: DataFrame, joinColumn: String): DataFrame = {
    handleException[DataFrame]({
      val joined = df1.join(df2, joinColumn, "inner")
      if (joined.columns.contains(joinColumn)) {
        joined.select(df1.columns.map(col): _*)
      } else {
        df1
      }
    }, df1)
  }

  def outerJoin(df1: DataFrame, df2: DataFrame, joinColumn: String): DataFrame = {
    handleException[DataFrame]({
      val joined = df1.join(df2, joinColumn, "outer")
      if (joined.columns.contains(joinColumn)) {
        joined.select(df1.columns.map(col): _*)
      } else {
        df1
      }
    }, df1)
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

<<<<<<< Updated upstream
   // Fonction pour ajouter une colonne au DataFrame
=======
  def updateRows(df: DataFrame, condition: Column, columnName: String, newValue: Any): DataFrame = {
    handleException[DataFrame]({
        df.withColumn(columnName, when(condition, lit(newValue)).otherwise(col(columnName)))
    },df)
  }
  
  def printSchema(df:DataFrame):Unit = {
    handleException[Unit]({
      df.printSchema()
    },())
  }

  // Fonction pour ajouter une colonne au DataFrame
>>>>>>> Stashed changes
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

<<<<<<< Updated upstream

  // Fonction pour créer un DataFrame à partir d'un fichier JSON
=======
   // Fonction pour créer un DataFrame à partir d'un fichier JSON
>>>>>>> Stashed changes
  def createDataFrameFromJson(jsonPath: String): Option[DataFrame] = {
    handleException[Option[DataFrame]]({
      Some(sparkSession.read.option("mode", "DROPMALFORMED").json(jsonPath))
    }, None)
  }

  // Fonction pour créer un DataFrame à partir d'une séquence de maps
  def createDataFrameFromSeq(seq: Seq[Product], schema: StructType): DataFrame = {
<<<<<<< Updated upstream
    val spark = SparkSession.builder().getOrCreate()
    val rdd = spark.sparkContext.parallelize(seq.map(Row.fromTuple))
    spark.createDataFrame(rdd, schema)
=======
    handleException[DataFrame]({
        val rdd = sparkSession.sparkContext.parallelize(seq.map(Row.fromTuple))
        sparkSession.createDataFrame(rdd, schema)
    },emptyDF)
  }

  
  // Fonction pour créer un DataFrame à partir d'un ResultSet
  def createDataFrameFromResultSet(resultSet: ResultSet): DataFrame = {
    handleException[DataFrame]({
      val metaData = resultSet.getMetaData
      val schema = StructType((1 to metaData.getColumnCount).map(i => StructField(metaData.getColumnName(i), DataTypes.StringType)))
      val values = (1 to metaData.getColumnCount).map(resultSet.getObject)
      val row = Row.fromSeq(values)
      val rows = Seq(row).asJava // Convertir la séquence de lignes en une liste de lignes
      val df = sparkSession.createDataFrame(rows, schema)
      df
    }, emptyDF)
>>>>>>> Stashed changes
  }
}
