package ccf

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object CCFRDD {
  def main(args: Array[String]): Unit = {
    // Création de la session Spark
    val spark = SparkSession.builder()
      .appName("CCF Scala RDD")
      .master("local[*]") // Exécution locale avec tous les cœurs de la machine
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    // Liste des graphes à analyser (format CSV)
    val files = Seq(
      ("data/G1_1k.csv", "G1"),
      ("data/G2_5k.csv", "G2"),
      ("data/G3_8k.csv", "G3"),
      ("data/G4_10k.csv", "G4")
    )

    // On stockera ici les résultats pour chaque graphe
    var results = Seq.empty[(String, Long, Long, Int, Double)]

    // Boucle sur chaque fichier
    files.foreach { case (path, label) =>
      println(s"\n📎 Analyse du graphe $label ($path)")

      // Lecture du fichier CSV brut
      val raw = sc.textFile(path)

      // On filtre l'en-tête et on extrait les arêtes sous forme de paires (source, target)
      val edges = raw
        .filter(!_.startsWith("source")) // Ignorer la première ligne
        .map(_.split(","))               // Découpage CSV
        .map(arr => (arr(0).toInt, arr(1).toInt)) // Conversion en entiers

      val nbEdges = edges.count() // Nombre total d’arêtes
      val nbNodes = edges.flatMap(e => Seq(e._1, e._2)).distinct().count() // Sommets uniques

      // Lancement de l’algorithme CCF version RDD
      val (components, iters, duration) = ccfRdd(edges)

      println(s"🔁 Nombre d'itérations : $iters")
      println(f"⏱️ Temps d'exécution : $duration%.3f sec")
      println("-" * 40)

      // On ajoute les résultats pour ce graphe
      results = results :+ (label, nbNodes, nbEdges, iters, duration)
    }

    // Affichage du résumé des performances
    val dfResults = results.toDF("Graphe", "Noeuds", "Arêtes", "Itérations", "Temps (s)")
    println("\n✅ Résumé des performances :")
    dfResults.show(truncate = false)

    // Arrêt de la session Spark
    spark.stop()
  }

  // Implémentation de l'algorithme Connected Components (CCF) en RDD
  def ccfRdd(edges: RDD[(Int, Int)], maxIters: Int = 50): (RDD[(Int, Int)], Int, Double) = {
    // On commence par éliminer les doublons (on veut que (2,3) et (3,2) soient traités comme une seule arête)
    val dedupedEdges = edges
      .map { case (a, b) =>
        if (a < b) (a, b) else (b, a) // On trie chaque paire pour pouvoir supprimer les doublons
      }
      .distinct()

    // Initialisation : chaque nœud commence dans sa propre composante
    var components = dedupedEdges
      .flatMap(e => Seq(e._1, e._2))
      .distinct()
      .map(n => (n, n)) // (nœud, composante)

    // Construction de la table des voisins pour chaque nœud
    val neighbors = dedupedEdges
      .flatMap { case (a, b) => Seq((a, b), (b, a)) } // Graphe non-orienté
      .groupByKey()
      .mapValues(_.toList)

    var converged = false
    var i = 0
    val startTime = System.nanoTime()

    // Boucle d'itérations de l’algorithme
    while (!converged && i < maxIters) {
      // Pour chaque nœud, on récupère sa composante actuelle et on la propage à ses voisins
      val joined = neighbors.join(components)

      val propagated = joined.flatMap { case (node, (neighs, comp)) =>
        (neighs :+ node).map(n => (n, comp)) // Chaque voisin (et le nœud lui-même) reçoit la composante
      }

      // Chaque nœud garde la plus petite composante reçue
      val newComponents = propagated.reduceByKey(math.min)

      // On compte combien de nœuds ont changé de composante par rapport à l’itération précédente
      val changes = components.join(newComponents)
        .filter { case (_, (oldC, newC)) => oldC != newC }
        .count()

      // Si aucun changement, on a convergé
      converged = (changes == 0)
      components = newComponents
      i += 1
    }

    val durationSec = (System.nanoTime() - startTime) / 1e9

    // On retourne les composantes finales, le nombre d'itérations, et le temps d'exécution
    (components, i, durationSec)
  }
}
