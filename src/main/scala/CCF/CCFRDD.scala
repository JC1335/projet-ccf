package ccf

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object CCFRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CCF Scala RDD")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    // 📂 Liste des fichiers à traiter
    val files = Seq(
      ("data/G1_1k.csv", "G1"),
      ("data/G2_5k.csv", "G2"),
      ("data/G3_8k.csv", "G3"),
      ("data/G4_10k.csv", "G4")
    )

    // Résultats accumulés
    var results = Seq.empty[(String, Long, Long, Int, Double)]

    files.foreach { case (path, label) =>
      println(s"\n📎 Graphe $label ($path)")

      val raw = sc.textFile(path)

      // Charger edges (source,target)
      val edges = raw
        .filter(!_.startsWith("source"))
        .map(_.split(","))
        .map(arr => (arr(0).toInt, arr(1).toInt))

      val nbEdges = edges.count()
      val nbNodes = edges.flatMap(e => Seq(e._1, e._2)).distinct().count()

      // Exécuter CCF avec dé-duplication
      val (components, iters, duration) = ccfRdd(edges)

      println(s"🔁 Itérations : $iters")
      println(f"⏱️ Temps : $duration%.3f sec")
      println("-" * 40)

      // Ajouter aux résultats
      results = results :+ (label, nbNodes, nbEdges, iters, duration)
    }

    // 📊 Synthèse finale en DataFrame
    val dfResults = results.toDF("Graphe", "Noeuds", "Arêtes", "Itérations", "Temps (s)")
    println("\n✅ Résumé des performances :")
    dfResults.show(truncate = false)

    spark.stop()
  }

  // 🧠 Implémentation CCF avec dé-duplication
  def ccfRdd(edges: RDD[(Int, Int)], maxIters: Int = 50): (RDD[(Int, Int)], Int, Double) = {
    // 🧼 CCF-Dedup : éliminer les doublons (ex: (2,3) et (3,2))
    val dedupedEdges = edges
      .map { case (a, b) =>
        if (a < b) (a, b) else (b, a)
      }
      .distinct()

    // Initialisation : chaque nœud appartient à sa propre composante
    var components = dedupedEdges
      .flatMap(e => Seq(e._1, e._2))
      .distinct()
      .map(n => (n, n))

    // Construction de la liste des voisins
    val neighbors = dedupedEdges
      .flatMap { case (a, b) => Seq((a, b), (b, a)) }
      .groupByKey()
      .mapValues(_.toList)

    var converged = false
    var i = 0
    val startTime = System.nanoTime()

    // Boucle d'itération
    while (!converged && i < maxIters) {
      val joined = neighbors.join(components)

      val propagated = joined.flatMap { case (node, (neighs, comp)) =>
        (neighs :+ node).map(n => (n, comp))
      }

      val newComponents = propagated.reduceByKey(math.min)

      val changes = components.join(newComponents)
        .filter { case (_, (oldC, newC)) => oldC != newC }
        .count()

      converged = (changes == 0)
      components = newComponents
      i += 1
    }

    val durationSec = (System.nanoTime() - startTime) / 1e9
    (components, i, durationSec)
  }
}
