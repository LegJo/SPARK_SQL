import java.util.Properties
import java.sql.SQLException
import org.apache.spark.sql.{SparkSession, DataFrame, Column, Row, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.{DriverManager,Connection}

// !!!!!! To run sbt server with jdk11 : sbt -java-home "C:\Program Files\Java\jdk-11"  !!!!!

object configSQLServer {
  private val SQLServerDriver:String = "oracle.jdbc.driver.OracleDriver"
  private val jdbcHostname:String = "localhost";
  private val jdbcPort:Int = 1521; 
  private val jdbcSID:String = "xe"; // SID de la base de données Oracle
  private val jdbcUsername:String = "system";
  private val jdbcPassword:String = "dbadmin";
  val schema = jdbcUsername
    
  Class.forName(SQLServerDriver)
  val jdbcUrl:String = s"jdbc:oracle:thin:@${jdbcHostname}:${jdbcPort}:${jdbcSID}"
  val connectionProperties:Properties = new Properties() {{
      put("url", jdbcUrl);
      put("user", jdbcUsername);
      put("password", jdbcPassword);
      put("driver", SQLServerDriver);
  }};
  val connection:Connection = DriverManager.getConnection(jdbcUrl, connectionProperties)
}


object SparkSQLApp {
  val sparkSession:SparkSession = {
    println("Creating Spark session...")
    SparkSession.builder()
      .master("local[1]")
      .appName("SparkSQL")
      .config("spark.log.level", "ERROR")
      .getOrCreate();
  }

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._
    import configSQLServer._
    import utils._
    import DataFrameFunctions._

    printLine()
    printColoredText("cyan",s"jdbcUrl: ${jdbcUrl}")
    printColoredText("cyan",s"Connection: ${connectionProperties}")
    printLine()

    //nom des table exemple
    val table_film:String = schema + ".film"
    val table_copy_film:String = schema + ".copy_film"
    val table_JSON:String = schema + ".JSON_TABLE"

    //Liste des tables en database
    printColoredText("cyan", "Table in Database :")
    SQLStatements.listTables()
    printLine()

    // Récupérer le DataFrame depuis la base de données
    var selectedDF:DataFrame = getDFFromOptionDF(SQLStatements.selectTable(table_film))
    printColoredText("green", "DataFrame From table " + table_film +" :")
    printDataFrame(selectedDF)

    //Récupérer un DataFrame depuis un JSON
    var selectedDFFromJSON:DataFrame = getDFFromOptionDF(createDataFrameFromJson(".\\src\\data\\data.json"))
    printColoredText("green", "DataFrame from JSON :")
    printDataFrame(selectedDFFromJSON)

    //Creation de la table avec le DataFrame du JSON
    SQLStatements.createTable(table_JSON, selectedDFFromJSON)
    //Select & Affiche la table creer
    selectedDFFromJSON = getDFFromOptionDF(SQLStatements.selectTable(table_JSON))
    printColoredText("green", "DataFrame From table " + table_JSON +" :")
    printDataFrame(selectedDFFromJSON)

    //copie et select de la copie de la table film
    SQLStatements.copyTableForced(table_film, table_copy_film)
    var selectedDFCopyFilm:DataFrame = getDFFromOptionDF(SQLStatements.selectTable(table_copy_film))
    printColoredText("green", "DataFrame From table " + table_copy_film +" :")
    printDataFrame(selectedDFCopyFilm)
    //Montrer que la table est copié

    //Insertion de donné depuis un Seq dans la table copie film
    //recuperation du schema depuis le dataframe
    val schemaCopyFilm = StructType(Seq(
      StructField("num_ind", IntegerType, nullable = false),
      StructField("titre", StringType, nullable = false),
      StructField("genre", StringType, nullable = false),
      StructField("annee", IntegerType, nullable = false)
    ))
    val seqToInsert = Seq((777, "Spark Le Film", "SparkGenre", 2025))
    SQLStatements.insertInTable(table_copy_film, createDataFrameFromSeq(seqToInsert, schemaCopyFilm))
    //Affichage de la table copy film apres insertion
    selectedDFCopyFilm = getDFFromOptionDF(SQLStatements.selectTable(table_copy_film))
    printColoredText("green", "DataFrame From table " + table_copy_film +" Apres Insertion:")
    printDataFrame(selectedDFCopyFilm)

    //Truncate de la table copie film
    SQLStatements.truncateTable(table_copy_film)
    selectedDFCopyFilm = getDFFromOptionDF(SQLStatements.selectTable(table_copy_film))
    //Affichage de la table copy film apres truncate
    printColoredText("green", "DataFrame From table " + table_copy_film +" After Truncate:")
    printDataFrame(selectedDFCopyFilm)

    //drop de la table copie film
    SQLStatements.dropTable(table_copy_film)
    //Montrer que la table est drop

    ///////////////////////////////
    //Fonctions sur les DataFrame//
    ///////////////////////////////

    var DF:DataFrame = selectedDFFromJSON

    //Execution de n'importe quelle statemnt SQL sur un DataFrame 
    val JSONViewName:String = "film_JSON"
    val anyStatementDF: DataFrame = DataFrameFunctions.executeAnyStatementOnDF(
      "SELECT " +
        "AVG(ANNEE) AS MOY_ANNEE, GENRE " +
        "FROM " + JSONViewName + " GROUP BY GENRE;",
        DF, JSONViewName    
      )
    printColoredText("green", "Execution de n'importe quelle statemnt SQL sur un DataFrame :")
    printDataFrame(anyStatementDF)

    // Filtrer les données avec une condition spécifique
    val filteredDataFrame: DataFrame = DataFrameFunctions.filter(DF, "ANNEE > 1999")
    printColoredText("green", "Filtered dataframe :")
    printDataFrame(filteredDataFrame)

    // Ordonner les données par colonne
    val orderedDataFrame: DataFrame = DataFrameFunctions.orderby(DF, "ANNEE")
    printColoredText("green", "Dataframe ordonné:")
    printDataFrame(orderedDataFrame)

    // Sélectionner des colonnes spécifiques
    val selectedDataFrame: DataFrame = DataFrameFunctions.select(DF, "ANNEE", "TITRE")
    printColoredText("green", "DataFrame selectionner juste annee et titre :")
    printDataFrame(selectedDataFrame)

    // Grouper les données avec des agrégations
    val groupedDF = DataFrameFunctions.groupby(DF, "GENRE", ("ANNEE", "avg"), ("TITRE", "count"))
    printColoredText("green", "DataFrame average des annee et count grouper par genre :")
    printDataFrame(groupedDF)

    // Supprimer une colonne spécifique
    val droppedDataFrame: DataFrame = DataFrameFunctions.drop(DF, "ANNEE")
    printColoredText("green", "DataFrame apres drop d'une column (annee):")
    printDataFrame(droppedDataFrame)

    // Obtenir les valeurs distinctes dans le DataFrame
    val distinctDataFrame: DataFrame = DataFrameFunctions.distinct(DF)
    printColoredText("green", "DataFrame doublons supp avec Distinct:")
    printDataFrame(distinctDataFrame)

    // Effectuer une union des DataFrame
    val unionDataFrame: DataFrame = DataFrameFunctions.union(DF, DF)
    printColoredText("green", "DataFrame union avec lui meme :")
    printDataFrame(unionDataFrame)

    // Limiter le nombre de lignes dans le DataFrame
    val limitedDataFrame: DataFrame = DataFrameFunctions.limit(DF, 2)
    printColoredText("green", "Top 2 du Dataframe:")
    printDataFrame(limitedDataFrame)

    // Obtenir des statistiques sur les colonnes du DataFrame
    val colStatsDataFrame: DataFrame = DataFrameFunctions.colStats(DF)
    printColoredText("green", "Stats du DataFrame:")
    printDataFrame(colStatsDataFrame)

    // Affichage du schéma du dataframe
    printColoredText("green", "Schema du DataFrame :")
    printSchema(DF)
    printLine()

    // Supprimer des lignes du DataFrame en fonction d'une condition
    val deleteRowDataFrame: DataFrame = DataFrameFunctions.deleteRows(DF, "ANNEE = 2002")
    printColoredText("green", "DataFrame avec les ligne Annee = 2002 delete :")
    printDataFrame(deleteRowDataFrame)

    // Truncate d'un DataFrame pour le vider
    val originalDF = sparkSession.createDataFrame(Seq((1, "A"), (2, "B")))
    val truncatedDF = DataFrameFunctions.truncate(originalDF)
    assert(truncatedDF.isEmpty, "truncate: Le DataFrame doit être vide après avoir été tronqué.")
    printColoredText("green", "DataFrame apres truncate :")
    printDataFrame(truncatedDF)

    // Effectuer une jointure gauche entre deux DataFrame
    val df1 = sparkSession.createDataFrame(Seq((1, "A"), (2, "B"))).toDF("id", "value1")
    val df2 = sparkSession.createDataFrame(Seq((1, "X"), (3, "Y"))).toDF("id", "value2")
    val joinedDF = DataFrameFunctions.leftJoin(df1, df2, "id")
    val expectedSchema = df1.schema
    printColoredText("green", "DataFrame apres leftJoin :")
    printDataFrame(joinedDF)
    assert(joinedDF.schema.equals(expectedSchema), "leftJoin: Le schéma du DataFrame résultant doit être identique à celui du DataFrame de gauche.")

    // Renommer une colonne dans le DataFrame
    val renamedDF = DataFrameFunctions.renameColumn(df1, "id", "newId")
    assert(renamedDF.schema.fieldNames.contains("newId"), "renameColumn: Le nom de la colonne doit être changé.")
    printColoredText("green", "DataFrame avec Column renommer :")
    printDataFrame(renamedDF)

    // Convertir le type d'une colonne dans le DataFrame
    val originalDF2 = sparkSession.createDataFrame(Seq(("1", "A"), ("2", "B"))).toDF("id", "value")
    val convertedDF = DataFrameFunctions.convertColumn(originalDF2, "id", IntegerType)
    assert(convertedDF.schema.fields(0).dataType == org.apache.spark.sql.types.IntegerType, "convertColumn: Le type de données de la colonne doit être converti en IntegerType.")
    printColoredText("green", "DataFrame avec id convertie en int :")
    printDataFrame(convertedDF)

    // Agréger les données dans le DataFrame
    val originalDF3 = sparkSession.createDataFrame(Seq((1, 10), (2, 20), (3, 30))).toDF("id", "value")
    val aggregatedDF = DataFrameFunctions.computeAggregate(originalDF3, "value", "sum")
    printColoredText("green", "DataFrame aggregation somme :")
    printDataFrame(aggregatedDF)
    val expectedSum = originalDF3.agg(sum(col("value"))).collect()(0)(0)
    val resultSum = aggregatedDF.collect()(0)(0)
    assert(resultSum == expectedSum, "computeAggregate: La somme calculée doit être correcte.")

    // Ajouter une colonne avec des valeurs spécifiques au DataFrame
    val valuesToAdd = Seq("value1", "value2", "value3")
    val dfWithNewColumn = addColumn(DF, "newColumn", StringType, valuesToAdd)
    printColoredText("green", "DataFrame avec Column newColumn ajoutée :")
    printDataFrame(dfWithNewColumn)

    // Créer un DataFrame à partir d'un fichier JSON
    val jsonPath = ".\\src\\data\\data.json"
    val jsonDF = getDFFromOptionDF(createDataFrameFromJson(jsonPath))
    printColoredText("green", "DataFrame creer depuis JSON :")
    printDataFrame(jsonDF)

    // Définir le schéma pour créer un DataFrame
    val schema2 = StructType(Seq(
      StructField("num_ind", IntegerType, nullable = false),
      StructField("titre", StringType, nullable = false),
      StructField("genre", StringType, nullable = false),
      StructField("annee", IntegerType, nullable = false)
    ))

    // Créer le Seq de données à partir du schéma
    val seqData = Seq((777, "Spark Le Film", "SparkGenre", 2025))
    val dfFromSeq: DataFrame = createDataFrameFromSeq(seqData, schema2)
    printColoredText("green", "DataFrame creer depuis Seq :")
    printDataFrame(dfFromSeq)

    //Update les ligne avec filtre
    val condition: Column = col("ANNEE") === 2002
    val updatedDF: DataFrame = updateRows(jsonDF, col("ANNEE") === 2002, "TITRE", "New Title")
    printColoredText("green", "DataFrame creer depuis JSON avec titre updated :")
    printDataFrame(updatedDF)

    // Partie JDBC

    // val selectedTableDataFrame: Option[DataFrame] = SQLStatements.selectTable("system.film")
    // printDataFrame(getDFFromOptionDF(selectedTableDataFrame))
    // val selectedTableDataFrameWithFilter: Option[DataFrame] = SQLStatements.selectTable("system.film", "titre", "ANNEE > 2000")
    // printDataFrame(getDFFromOptionDF(selectedTableDataFrameWithFilter))

    // val newDF = Seq(
    //   (15, 20 , "Interstellar", "Science Fiction", 2016),
    //   (16, 21 , "Inception", "Science Fiction", 2010)
    // ).toDF("NUM_FILM", "NUM_IND", "TITRE", "GENRE", "ANNEE")
    // SQLStatements.createTable("system.newtable", newDF)
    // printDataFrame(getDFFromOptionDF(SQLStatements.selectTable("system.newtable")))

    // val anotherDF = Seq(
    //   (14, 19 , "Interstellar1", "Science Fiction", 2014)
    // ).toDF("NUM_FILM", "NUM_IND", "TITRE", "GENRE", "ANNEE")
    // SQLStatements.overwriteTable("system.newtable", anotherDF)
    // printDataFrame(getDFFromOptionDF(SQLStatements.selectTable("system.newtable")))

    // val another2DF = Seq(
    //   (17, 22 , "Interstellar2", "Science Fiction", 2018)
    // ).toDF("NUM_FILM", "NUM_IND", "TITRE", "GENRE", "ANNEE")
    // SQLStatements.insertInTable("system.newtable", another2DF)
    // printDataFrame(getDFFromOptionDF(SQLStatements.selectTable("system.newtable")))

    // SQLStatements.truncateTable("system.newtable")
    // printDataFrame(getDFFromOptionDF(SQLStatements.selectTable("system.newtable")))

    // Appeler la procédure stockée
    SQLStatements.callStoredProcedure("select_dramatic_films")

    println("Stopping Spark session...")
    connection.close()
    sparkSession.stop()
  }
}
