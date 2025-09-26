package ccf

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._

object CCFGraphX {
  def main(args: Array[String]): Unit = {
    // Démarrage de la session Spark
    val spark = SparkSession.builder()
      .appName("CCF GraphX")
      .master("local[*]") // Mode local pour l'exécution sur une seule machine
      .getOrCreate()

    import spark.implicits._

    // Liste des fichiers à analyser (chaque fichier représente un graphe)
    val files = Seq(
      ("data/G1_1k.csv", "G1"),
      ("data/G2_5k.csv", "G2"),
      ("data/G3_8k.csv", "G3"),
      ("data/G4_10k.csv", "G4")
    )

    // Contiendra les résultats pour chaque graphe
    var results = Seq.empty[(String, Long, Long, Double)]

    // On traite les fichiers un par un
    files.foreach { case (path, label) =>
      println(s"\n📎 Traitement du graphe $label ($path)")

      // Chargement du fichier CSV avec Spark
      val df = spark.read
        .option("header", "true")       // Les fichiers ont des en-têtes
        .option("inferSchema", "true")  // On laisse Spark deviner les types de colonnes
        .csv(path)
        .toDF("source", "target")
        .withColumn("source", col("source").cast("long"))
        .withColumn("target", col("target").cast("long"))
        .na.drop() // Suppression des lignes incomplètes s’il y en a

      val nbEdges = df.count() // Nombre d’arêtes
      val nbNodes = df.select("source").union(df.select("target")).distinct().count() // Sommets uniques

      // Exécution de l'algorithme CCF avec GraphX
      val (components, duration) = ccfGraphX(df, spark)

      println(f" Temps d'exécution : $duration%.3f sec")
      println("-" * 40)

      // Ajout des résultats à la liste
      results = results :+ (label, nbNodes, nbEdges, duration)
    }

    // Affichage du résumé final
    val dfResults = results.toDF("Graphe", "Noeuds", "Arêtes", "Temps (s)")
    println("\n Résumé des performances (GraphX) :")
    dfResults.show(truncate = false)

    println("\n Fin du programme CCF GraphX")
    spark.stop()
  }

  // Fonction qui exécute le CCF en utilisant l’API GraphX
  def ccfGraphX(df: DataFrame, spark: SparkSession): (DataFrame, Double) = {
    import spark.implicits._

    val startTime = System.nanoTime()

    // Transformation du DataFrame en RDD d’arêtes compatibles avec GraphX
    val edgesRDD: RDD[Edge[Long]] = df.rdd.map(row => {
      val src = row.getAs[Long]("source")
      val dst = row.getAs[Long]("target")
      Edge(src, dst)
    })

    // Construction du graphe à partir des arêtes
    val graph = Graph.fromEdges(edgesRDD, defaultValue = 1L)

    // Utilisation de l’algorithme Connected Components de GraphX
    val cc = graph.connectedComponents()

    // Résultat sous forme de DataFrame (nœud, composante)
    val componentsDF = cc.vertices.toDF("node", "component")

    // On déclenche l'exécution pour mesurer le temps réel (sinon c’est paresseux)
    componentsDF.count()

    val duration = (System.nanoTime() - startTime) / 1e9

    (componentsDF, duration)
  }
}
