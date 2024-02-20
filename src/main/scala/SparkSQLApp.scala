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
  private val jdbcPassword:String = "insertPassword";
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
    
    val dfOfTableToCreate:DataFrame = Seq((777, "Spark Le Film", "SparkGenre", 2025)).toDF("num_ind", "titre", "genre", "annee");
    SQLStatements.createTable("system.film_test", dfOfTableToCreate);
    selectedData = SQLStatements.selectTable("system.film_test");    

    val dfToInsert:DataFrame = Seq((777888, "Spark Le Film 2", "SparkGenre", 2025)).toDF("num_ind", "titre", "genre", "annee");
    SQLStatements.insertInTable("dbo.film_test", dfToInsert);
    selectedData = SQLStatements.selectTable("dbo.film_test");
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
    // Regrouper les lignes du DataFrame en fonction des valeurs d'une colonne spécifiée (par exemple, la colonne "GENRE")
    // val groupedDataFrame = customDataFrame.map(_.groupBy("GENRE"))
    // groupedDataFrame.foreach { groupMap =>
    //   groupMap.foreach { case (value, df) =>
    //     println(s"Group $value:")
    //     df.show()
    //   }
    // }


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

    // TEST TRUNCATE :
    // Vérifier si customDataFrame contient une valeur CustomDataFrame
    customDataFrame match {
      case Some(df) =>
        // Appeler les méthodes sur df
        // TEST TRUNCATE :
        val truncatedDataFrame = df.truncate()
        truncatedDataFrame.show()

        // TEST UPDATEROWS :
        // Définir un filtre pour les lignes à mettre à jour
        val filter: Map[String, Any] => Boolean = row => {
          val annee = row("ANNEE")
          if (annee.isInstanceOf[java.math.BigDecimal]) {
            val anneeBigDecimal = annee.asInstanceOf[java.math.BigDecimal]
            anneeBigDecimal.intValue() < 2000
          } else {
            false // Gérer les autres types si nécessaire
          }
        }
        // Définir les valeurs de mise à jour
        val updateValues = Map("GENRE" -> "Nouveau Genre")
        val updatedDataFrame = df.updateRows(filter, updateValues)
        updatedDataFrame.show()

        // TEST DELETEROWS :
        // Définir un filtre pour les lignes à supprimer
        val deleteFilter: Map[String, Any] => Boolean = row => {
          val annee = row("ANNEE")
          annee match {
            case _: java.math.BigDecimal => annee.asInstanceOf[java.math.BigDecimal].intValue() < 2000
            case _: java.lang.Integer => annee.asInstanceOf[Int] < 2000
            case _ => false // Gérer les autres types si nécessaire
          }
        }
        val deletedDataFrame = df.deleteRows(deleteFilter)
        deletedDataFrame.show()

        // TEST LEFTJOIN :
        val leftJoinDataFrame = df.leftJoin(df, "ANNEE")
        leftJoinDataFrame.show()

        // TEST RIGHTJOIN :
        val rightJoinDataFrame = df.rightJoin(df, "ANNEE")
        rightJoinDataFrame.show()

        // TEST INNERJOIN :
        val innerJoinDataFrame = df.innerJoin(df, "ANNEE")
        innerJoinDataFrame.show()

        // TEST FULLOUTERJOIN :
        val fullOuterJoinDataFrame = df.fullOuterJoin(df, "ANNEE")
        fullOuterJoinDataFrame.show()

      case None =>
        println("customDataFrame is empty.")
    }




    // Arrêter la session Spark
    sparkSession.stop()
  }
}
