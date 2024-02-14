import java.util.Properties
import java.sql.SQLException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode

// !!!!!! To run sbt server with jdk11 : sbt -java-home "C:\Program Files\Java\jdk-11"  !!!!! 

object configSQLServer {
  private val jdbcHostname:String = "localhost";
  private val jdbcPort:Int = 1521; 
  private val jdbcSID:String = "xe"; // SID de la base de données Oracle
  private val jdbcUsername:String = "system";
  private val jdbcPassword:String = "dbadmin";
  Class.forName("oracle.jdbc.driver.OracleDriver")
  val jdbcUrl:String = s"jdbc:oracle:thin:@${jdbcHostname}:${jdbcPort}:${jdbcSID}"
  val connectionProperties:Properties = new Properties() {{
    put("user", jdbcUsername)
    put("password", jdbcPassword)
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
    var selectedData:Option[DataFrame] = SQLStatements.select("system.film");
    printDataFrame(selectedData);

    val customDataFrame: Option[CustomDataFrame] = selectedData.map(df =>
      CustomDataFrame(
        df.collect().map(row =>
          Map(
            "NUM_FILM" -> row.getAs[Double]("NUM_FILM"),
            "NUM_IND" -> row.getAs[Double]("NUM_IND"),
            "TITRE" -> row.getAs[String]("TITRE"),
            "GENRE" -> row.getAs[String]("GENRE"),
            "ANNEE" -> row.getAs[Double]("ANNEE")
          )
        ).toList
      )
    )

    // TEST FILTER :
    // Filtrer les données pour ne conserver que les films avec un ANNEE supérieur à 1999
    // val filteredDataFrame = customDataFrame.map(_.filter(row => row("ANNEE").asInstanceOf[java.math.BigDecimal].intValue() > 1999))
    // filteredDataFrame.foreach(_.show())

    // TEST ORDERBY :
    // Appel de la fonction orderBy pour trier par colonne ANNEE
    // val orderedDataFrame = customDataFrame.map(_.orderBy("ANNEE")) 
    // orderedDataFrame.foreach(_.show())

    // TEST GROUP BY :
    // TEST PAS CONCLUANT

    // TEST SELECT:
    // val selectedDataFrame = customDataFrame.map(_.select("TITRE", "ANNEE"))
    // selectedDataFrame.foreach(_.show())

    // TEST DROP:
    // val newDataFrame = customDataFrame.map(_.drop("ANNEE"))
    // newDataFrame.foreach(_.show())

    // TEST DISTINCT:
    // val distinctDataFrame = customDataFrame.map(_.distinct())
    // distinctDataFrame.foreach(_.show())

    // TEST UNION
    // Données bidon pour tester l'union
    // val dummyData2 = List(
    //   Map("NUM_FILM" -> 3.0, "NUM_IND" -> 103.0, "TITRE" -> "Film 3", "GENRE" -> "Drame", "ANNEE" -> 2010.0),
    //   Map("NUM_FILM" -> 4.0, "NUM_IND" -> 104.0, "TITRE" -> "Film 4", "GENRE" -> "Horreur", "ANNEE" -> 2015.0)
    // )

    // val customDataFrame2 = CustomDataFrame(dummyData2)

    // // Appel de la fonction union pour tester
    // val unionDataFrame = customDataFrame.map(_.union(customDataFrame2))
    // unionDataFrame.foreach(_.show())

    // TEST LIMIT :
    // Limiter le DataFrame aux premières 'n' lignes (par exemple, 5 lignes)
    // val limitedDataFrame = customDataFrame.map(_.limit(5))
    // limitedDataFrame.foreach(_.show())

    // TEST COLUMNSTATS :
    // Calculer des statistiques sur une colonne numérique (par exemple, la colonne "ANNEE")
    // val statsDataFrame = customDataFrame.map(_.columnStats("ANNEE"))
    // statsDataFrame.foreach(_.show())

    // TEST JOIN :
    // Effectuer une jointure avec un autre DataFrame sur une colonne spécifiée
    // val joinedDataFrameOption = customDataFrame.map(df => df.join(df, "COLUMN_NAME"))
    // joinedDataFrameOption.foreach(_.show())


    // TEST FILLMISSINGVALUES :
    // Remplacer les valeurs manquantes dans une colonne par une valeur par défaut (par exemple, la colonne "ANNEE" avec la valeur par défaut 0)
    // val filledDataFrame = customDataFrame.map(_.fillMissingValues("ANNEE", 0))
    // filledDataFrame.foreach(_.show())

    // TEST RENAMECOLUMN :
    // Renommer une colonne (par exemple, renommer la colonne "ANNEE" en "YEAR")
    // val renamedDataFrame = customDataFrame.map(_.renameColumn("ANNEE", "YEAR"))
    // renamedDataFrame.foreach(_.show())

    // TEST WITHROWID :
    // Ajouter un identifiant unique à chaque ligne
    // val rowIdDataFrame = customDataFrame.map(_.withRowId)
    // rowIdDataFrame.foreach(_.show())

    // Arrêter la session Spark
    sparkSession.stop()
  }
}
