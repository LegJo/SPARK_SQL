import org.apache.spark.sql.{DataFrame, SparkSession}

object utils {
    def printLine(): Unit = {
         println("=" * 60);
    }

    def printDataFrame(df:Option[DataFrame]): Unit = {
        df match {
            case None => println("None value was passed as a DataFrame in printDataFrame");
            case _ => {
                df.foreach(dataRow => dataRow.show());
                printLine();
            }
        }
    }
}

