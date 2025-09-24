import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object DataFrame_Pure_CCF_Corrected {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CCF Pure DataFrame Corrigé")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val edges = spark.read
      .option("delimiter", " ")
      .csv("src/main/resources/Edges.txt")
      .toDF("src", "dst")

    println("✅ Arêtes chargées (Aperçu) :")
    edges.show()

    val srcVertices = edges.select($"src".as("node"))
    val dstVertices = edges.select($"dst".as("node"))
    var labels = srcVertices.union(dstVertices).distinct().withColumn("label", col("node"))

    var iteration = 0
    var changesCount = -1L // Initialiser avec une valeur qui garantit l'entrée dans la boucle

    while (changesCount != 0) {
      iteration += 1

      // Propagation des labels : join avec les arêtes dans les deux sens
      val propagatedLabels = edges
        .join(labels, $"src" === $"node")
        .select($"dst".as("node"), $"label".as("propagated_label"))
        .union(
          edges
            .join(labels, $"dst" === $"node")
            .select($"src".as("node"), $"label".as("propagated_label"))
        )

      // Agréger pour trouver le minimum (le plus petit label) par sommet
      val newLabels = propagatedLabels
        .groupBy("node")
        .agg(min("propagated_label").as("label"))
        .alias("newLabels")

      // Mettre à jour les labels en gardant la meilleure valeur (la plus petite)
      val updatedLabels = labels.as("oldLabels")
        .join(newLabels, $"oldLabels.node" === $"newLabels.node")
        .select($"oldLabels.node", least($"oldLabels.label", $"newLabels.label").as("label"))

      // Vérifier si des labels ont changé
      // Il faut comparer les deux DataFrames de manière ordonnée pour être sûr
      val oldRows = labels.orderBy("node").collect()
      val newRows = updatedLabels.orderBy("node").collect()

      changesCount = oldRows.zip(newRows).count { case (oldRow, newRow) => oldRow.getAs[Int]("label") != newRow.getAs[Int]("label") }

      println(s"Itération $iteration : $changesCount labels ont changé.")

      labels = updatedLabels
    }

    println(s"\n✅ Composantes connexes trouvées après $iteration itérations :")
    labels.orderBy("node").show()

    spark.stop()
  }
}