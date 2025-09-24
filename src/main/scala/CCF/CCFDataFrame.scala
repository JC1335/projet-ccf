import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object CCFDataFrame {
  def main(args: Array[String]): Unit = {
    // Récupérer le master URL des arguments ou utiliser 'local[*]' par défaut pour les tests
    val masterUrl = if (args.length > 0) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("CCF Scala DataFrame")
      .master(masterUrl)
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

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

      // Charger le CSV
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .toDF("source", "target")
        .withColumn("source", col("source").cast("int"))
        .withColumn("target", col("target").cast("int"))
        .na.drop()

      val nbEdges = df.count()
      val nbNodes = df.select("source").union(df.select("target")).distinct().count()

      // Exécuter CCF DataFrame
      val (components, iters, duration) = ccfDF(df, spark)

      println(s"🔁 Itérations : $iters")
      println(f"⏱️ Temps : $duration%.3f sec")
      println("-" * 40)

      // Ajouter les résultats dans la séquence
      results = results :+ (label, nbNodes, nbEdges, iters, duration)
    }

    // 📊 Rapport de synthèse final
    val dfResults = results.toDF("Graphe", "Noeuds", "Arêtes", "Itérations", "Temps (s)")
    println("\n✅ Résumé des performances (DataFrame) :")
    dfResults.show(truncate = false)

    println("\n🏁 Fin du programme CCF DataFrame")
    spark.stop()
  }


  // 🧠 Algorithme CCF avec DataFrames
  def ccfDF(df: DataFrame, spark: SparkSession, maxIters: Int = 50): (DataFrame, Int, Double) = {
    import spark.implicits._

    var components = df.select($"source").union(df.select($"target")).distinct()
      .withColumnRenamed("source", "node")
      .withColumn("comp", $"node")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val neighbors = df.union(df.select($"target".as("source"), $"source".as("target")))

    var converged = false
    var i = 0
    val startTime = System.nanoTime()

    while (!converged && i < maxIters) {
      val joined = neighbors.join(components, $"source" === $"node")

      val propagated = joined.select($"target".as("node"), $"comp")
        .union(joined.select($"source".as("node"), $"comp"))

      val newComponents = propagated.groupBy("node").agg(min("comp").as("comp"))

      val changes = components.join(newComponents, Seq("node"))
        .filter(components("comp") =!= newComponents("comp"))
        .count()

      val oldComponents = components
      components = newComponents.persist(StorageLevel.MEMORY_AND_DISK)
      oldComponents.unpersist()

      converged = (changes == 0)
      i += 1
    }

    val durationSec = (System.nanoTime() - startTime) / 1e9

    // Unpersiste le dernier RDD mis en cache
    components.unpersist()

    (components, i, durationSec)
  }
}