import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object CCFDataFrame {
  def main(args: Array[String]): Unit = {
    // On récupère l'URL du master Spark passée en argument, ou bien on travaille en local par défaut (pratique pour les tests)
    val masterUrl = if (args.length > 0) args(0) else "local[*]"

    // Initialisation de la session Spark
    val spark = SparkSession.builder()
      .appName("CCF Scala DataFrame")
      .master(masterUrl)
      .config("spark.sql.adaptive.enabled", "false") // Désactivation de l'optimisation adaptative pour garder le contrôle
      .getOrCreate()

    import spark.implicits._

    // Liste des fichiers CSV à analyser, chacun représentant un graphe avec un nombre d'arêtes différent
    val files = Seq(
      ("data/G1_1k.csv", "G1"),
      ("data/G2_5k.csv", "G2"),
      ("data/G3_8k.csv", "G3"),
      ("data/G4_10k.csv", "G4")
    )

    // On va stocker ici les résultats pour chaque graphe
    var results = Seq.empty[(String, Long, Long, Int, Double)]

    // On traite chaque fichier un par un
    files.foreach { case (path, label) =>
      println(s"\n📎 Traitement du graphe $label ($path)")

      // Chargement du fichier CSV avec Spark
      val df = spark.read
        .option("header", "true")       // Les fichiers ont des en-têtes
        .option("inferSchema", "true")  // Spark devine automatiquement le type des colonnes
        .csv(path)
        .toDF("source", "target")       // On renomme les colonnes pour plus de clarté
        .withColumn("source", col("source").cast("int"))  // On convertit les ID en entiers
        .withColumn("target", col("target").cast("int"))
        .na.drop() // On enlève les lignes avec des valeurs manquantes, si jamais

      // Nombre total d'arêtes (lignes)
      val nbEdges = df.count()

      // Pour avoir tous les sommets uniques, on fusionne les colonnes source et target, puis on déduplique
      val nbNodes = df.select("source").union(df.select("target")).distinct().count()

      // On applique l'algorithme CCF (Connected Components with DataFrame)
      val (components, iters, duration) = ccfDF(df, spark)

      // Quelques infos pour le suivi
      println(s"🔁 Nombre d'itérations effectuées : $iters")
      println(f"⏱️ Temps d'exécution : $duration%.3f secondes")
      println("-" * 40)

      // On sauvegarde les résultats pour ce graphe
      results = results :+ (label, nbNodes, nbEdges, iters, duration)
    }

    // Récapitulatif final des performances pour chaque graphe
    val dfResults = results.toDF("Graphe", "Noeuds", "Arêtes", "Itérations", "Temps (s)")
    println("\n✅ Résumé des performances (DataFrame) :")
    dfResults.show(truncate = false)

    println("\n🏁 Fin de l'exécution du programme CCF avec DataFrame")
    spark.stop()
  }


  // Fonction principale qui exécute l’algorithme des composantes connexes avec DataFrame
  def ccfDF(df: DataFrame, spark: SparkSession, maxIters: Int = 50): (DataFrame, Int, Double) = {
    import spark.implicits._

    // Étape 1 : initialisation - chaque nœud commence dans sa propre composante
    var components = df.select($"source").union(df.select($"target")).distinct()
      .withColumnRenamed("source", "node")
      .withColumn("comp", $"node") // Au début, chaque nœud est sa propre composante
      .persist(StorageLevel.MEMORY_AND_DISK)

    // On construit les voisins en doublant les liens dans les deux sens (non-orienté)
    val neighbors = df.union(df.select($"target".as("source"), $"source".as("target")))

    var converged = false
    var i = 0
    val startTime = System.nanoTime()

    // Boucle principale de l’algorithme
    while (!converged && i < maxIters) {
      // On propage les identifiants de composantes entre voisins
      val joined = neighbors.join(components, $"source" === $"node")

      val propagated = joined.select($"target".as("node"), $"comp")
        .union(joined.select($"source".as("node"), $"comp"))

      // Chaque nœud choisit le plus petit ID de composante reçu
      val newComponents = propagated.groupBy("node").agg(min("comp").as("comp"))

      // On vérifie si quelque chose a changé (si des nœuds ont changé de composante)
      val changes = components.join(newComponents, Seq("node"))
        .filter(components("comp") =!= newComponents("comp"))
        .count()

      // On met à jour les composantes
      val oldComponents = components
      components = newComponents.persist(StorageLevel.MEMORY_AND_DISK)
      oldComponents.unpersist()

      // Si plus aucun changement, on s’arrête
      converged = (changes == 0)
      i += 1
    }

    val durationSec = (System.nanoTime() - startTime) / 1e9

    // Libération de la mémoire (important avec Spark)
    components.unpersist()

    (components, i, durationSec)
  }
}
