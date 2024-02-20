//class contenant les fonctions permettant la manipulation des dataframe 

case class CustomDataFrame(data: List[Map[String, Any]]) {

  // **** Liste de toutes les fonctions ****************************************************************************
  //                                                                                                               *
  // filter : Filtrer les données selon une condition sur une colonne donnée                                       *
  // orderBy : Trie les lignes du DataFrame en fonction des colonnes spécifiées                                    *
  // groupBy : Regroupe les lignes du DataFrame en fonction des valeurs d'une colonne spécifiée                    *
  // select : Sélectionne les colonnes spécifiées pour chaque ligne du DataFrame                                   *
  // withColumn : Ajouter une colonne calculée basée sur d'autres colonnes                                         *
  // drop : Supprime les colonnes spécifiées du DataFrame                                                          *
  // distinct : Retourne un nouveau DataFrame avec des lignes distinctes                                           *
  // union : Effectue l'union avec un autre DataFrame, ajoutant toutes les lignes                                  *
  // limit : Limite le DataFrame aux premières 'n' lignes                                                          *
  // colonStats : Calculer des statistiques sur une colonne numérique                                              *
  // join : Effectue une jointure avec un autre DataFrame sur une colonne spécifiée                                *
  // fillWithMissingValues : Remplacer les valeurs manquantes dans une colonne par une valeur par défaut           *
  // renameColumn : Renommer une colonne                                                                           *
  // withRowId : Ajouter un identifiant unique à chaque ligne                                                      *
  // show : Affiche les données du DataFrame                                                                       *
  // truncate : Vide le DataFrame, supprime toutes les données                                                     *
  // updateRows : Met à jour les lignes du DataFrame en fonction d'un filtre et des valeurs de mise à jour         *
  // deleteRows : Supprime les lignes du DataFrame en fonction d'un filtre                                         *
  // leftJoin : Effectue un left join avec un autre DataFrame sur une colonne spécifiée                            *
  // rightJoin : Effectue un right join avec un autre DataFrame sur une colonne spécifiée                          *
  // innerJoin : Effectue un inner join avec un autre DataFrame sur une colonne spécifiée                          *
  // fullOuterJoin : Effectue un full outer join avec un autre DataFrame sur une colonne spécifiée                 *
  //                                                                                                               *
  // ***************************************************************************************************************


  // créer dataframe a partir de json //Je fais
  // créer dataframe a partir d'un type seq //
  // différence dataset vs dataframe, quel cas d'utilisation ? Faire fonctions pour les cas particuliers 

   // Fonction de vérification du DataFrame
    def checkDataFrameAndExecute(action: => CustomDataFrame): CustomDataFrame = {
        if (data.isEmpty) {
            println("DataFrame is empty. Cannot perform operation.")
            this // Renvoyer l'instance actuelle de CustomDataFrame
        } else {
            action
        }
    }

   // Filtrer les données selon une condition sur une colonne donnée
   def filter(predicate: Map[String, Any] => Boolean): CustomDataFrame = {
    checkDataFrameAndExecute(CustomDataFrame(data.filter(predicate)))
   }
  // Trie les lignes du DataFrame en fonction des colonnes spécifiées
  def orderBy(columns: String*)(implicit ord: Ordering[Seq[Any]]): CustomDataFrame = {
    if (data.isEmpty) {
      println("DataFrame is empty. Cannot perform orderBy.")
      this
    } else {
      val filteredColumns = columns.filter(col => data.head.contains(col))
      if (filteredColumns.isEmpty) {
        println("No valid columns specified for orderBy.")
        this
      } else {
        CustomDataFrame(data.sortBy(row => filteredColumns.map(row)))
      }
    }
  }

  // Regroupe les lignes du DataFrame en fonction des valeurs d'une colonne spécifiée
  def groupBy(column: String): Map[Any, CustomDataFrame] = {
    if (data.isEmpty) {
      println("DataFrame is empty. Cannot perform groupBy.")
      Map.empty
    } else {
      val filteredData = data.filter(_.contains(column))
      if (filteredData.isEmpty) {
        println("No valid rows with specified column for groupBy.")
        Map.empty
      } else {
        filteredData.groupBy(_.getOrElse(column, "Undefined")).mapValues(rows => CustomDataFrame(rows.toList)).toMap
      }
    }
  }

  // Sélectionne les colonnes spécifiées pour chaque ligne du DataFrame
  def select(columns: String*): CustomDataFrame = {
    if (data.isEmpty) {
      println("DataFrame is empty. Cannot perform select.")
      this
    } else {
      CustomDataFrame(data.map(row => row.filterKeys(columns.contains).toMap))
    }
  }

  // Ajouter une colonne calculée basée sur d'autres colonnes
  def withColumn(newColumn: String, calculation: Map[String, Any] => Any): CustomDataFrame = {
    CustomDataFrame(data.map(row => row + (newColumn -> calculation(row))))
  }

  // Supprime les colonnes spécifiées du DataFrame
  def drop(columns: String*): CustomDataFrame = {
    if (data.isEmpty) {
      println("DataFrame is empty. Cannot perform drop.")
      this
    } else {
      CustomDataFrame(data.map(row => row.filterKeys(!columns.contains(_)).toMap))
    }
  }

  // Retourne un nouveau DataFrame avec des lignes distinctes
  def distinct(): CustomDataFrame = {
    if (data.isEmpty) {
      println("DataFrame is empty. Cannot perform distinct.")
      this
    } else {
      CustomDataFrame(data.distinct)
    }
  }

  // Effectue l'union avec un autre DataFrame, ajoutant toutes les lignes
  def union(other: CustomDataFrame): CustomDataFrame = {
    if (data.isEmpty || other.data.isEmpty) {
      println("One of the DataFrames is empty. Cannot perform union.")
      this
    } else {
      CustomDataFrame(data ++ other.data)
    }
  }

   // Limite le DataFrame aux premières 'n' lignes
  def limit(n: Int): CustomDataFrame = {
    checkDataFrameAndExecute(CustomDataFrame(data.take(n)))
  }

  // Calculer des statistiques sur une colonne numérique
  def columnStats(column: String): CustomDataFrame = {
    checkDataFrameAndExecute {
        if (data.isEmpty) {
            println("DataFrame is empty. Cannot perform columnStats.")
            this
        } else {
            val values = data.flatMap(_.get(column).map(_.asInstanceOf[java.math.BigDecimal].doubleValue()))
            if (values.isEmpty) {
                println(s"No values found for column $column.")
                this
            } else {
                val mean = values.sum / values.length
                val variance = values.map(x => math.pow(x - mean, 2)).sum / values.length
                val stdDev = math.sqrt(variance)
                CustomDataFrame(List(
                    Map("min" -> values.min, "max" -> values.max, "stdDev" -> stdDev)
                ))
            }
        }
    }
  }

  // Effectue une jointure avec un autre DataFrame sur une colonne spécifiée
  def join(other: CustomDataFrame, column: String): CustomDataFrame = {
    checkDataFrameAndExecute {
      if (data.isEmpty || other.data.isEmpty) {
        println("One of the DataFrames is empty. Cannot perform join.")
        this
      } else {
        val joinedData = for {
          thisRow <- data
          thatRow <- other.data
          if thisRow.contains(column) && thatRow.contains(column) && thisRow(column) == thatRow(column)
        } yield thisRow ++ thatRow
        CustomDataFrame(joinedData)
      }
    }
  }

  // Remplacer les valeurs manquantes dans une colonne par une valeur par défaut
  def fillMissingValues(column: String, defaultValue: Any): CustomDataFrame = {
    checkDataFrameAndExecute(CustomDataFrame(data.map(row => row.updated(column, row.getOrElse(column, defaultValue)))))
  }

  // Renommer une colonne
  def renameColumn(oldName: String, newName: String): CustomDataFrame = {
    checkDataFrameAndExecute(CustomDataFrame(data.map(row => row.updated(newName, row(oldName)).- (oldName))))
  }

  // Ajouter un identifiant unique à chaque ligne
  def withRowId: CustomDataFrame = {
    checkDataFrameAndExecute(CustomDataFrame(data.zipWithIndex.map { case (row, index) => row + ("ROW_ID" -> index) }))
  }

  // Affiche les données du DataFrame
  def show(): Unit = {
    if (data.isEmpty) {
      println("DataFrame is empty. Nothing to show.")
    } else {
      println("DataFrame:")
      data.foreach(row => {
        println("  {")
        row.foreach { case (key, value) =>
          println(s"    $key : $value")
        }
        println("  }")
      })
    }
  }

  // Fonction pour vider un DataFrame (truncate)
    def truncate(): CustomDataFrame = {
    checkDataFrameAndExecute {
        CustomDataFrame(List.empty)
    }
    }

    // Fonction pour mettre à jour des lignes d'un DataFrame en fonction d'un filtre
    def updateRows(filter: Map[String, Any] => Boolean, updateValues: Map[String, Any]): CustomDataFrame = {
    checkDataFrameAndExecute {
        if (data.isEmpty) {
        println("DataFrame is empty. Cannot perform updateRows.")
        this
        } else {
        CustomDataFrame(data.map { row =>
            if (filter(row)) row ++ updateValues
            else row
        })
        }
    }
    }

    // Fonction pour supprimer des lignes d'un DataFrame en fonction d'un filtre
    def deleteRows(filter: Map[String, Any] => Boolean): CustomDataFrame = {
    checkDataFrameAndExecute {
        if (data.isEmpty) {
        println("DataFrame is empty. Cannot perform deleteRows.")
        this
        } else {
        CustomDataFrame(data.filterNot(filter))
        }
    }
    }

    // Left join
    def leftJoin(other: CustomDataFrame, column: String): CustomDataFrame = {
    checkDataFrameAndExecute {
        if (data.isEmpty || other.data.isEmpty) {
        println("One of the DataFrames is empty. Cannot perform join.")
        this
        } else {
        val joinedData = for {
            thisRow <- data
            thatRow <- other.data
            if thisRow.contains(column) && thatRow.contains(column) && thisRow(column) == thatRow(column)
        } yield thisRow ++ thatRow
        CustomDataFrame(joinedData)
        }
    }
    }

    // Right join
    def rightJoin(other: CustomDataFrame, column: String): CustomDataFrame = {
    other.leftJoin(this, column)
    }

    // Inner join
    def innerJoin(other: CustomDataFrame, column: String): CustomDataFrame = {
    checkDataFrameAndExecute {
        if (data.isEmpty || other.data.isEmpty) {
        println("One of the DataFrames is empty. Cannot perform join.")
        this
        } else {
        val joinedData = for {
            thisRow <- data
            thatRow <- other.data
            if thisRow.contains(column) && thatRow.contains(column) && thisRow(column) == thatRow(column)
        } yield thisRow ++ thatRow
        CustomDataFrame(joinedData)
        }
    }
    }

    // Full outer join
    def fullOuterJoin(other: CustomDataFrame, column: String): CustomDataFrame = {
    checkDataFrameAndExecute {
        if (data.isEmpty || other.data.isEmpty) {
        println("One of the DataFrames is empty. Cannot perform join.")
        this
        } else {
        val joinedData = for {
            thisRow <- data
            thatRow <- other.data
        } yield thisRow ++ thatRow
        CustomDataFrame(joinedData)
        }
    }
    }

}

