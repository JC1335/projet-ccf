package ccf

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._

object CCFGraphX {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CCF GraphX")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val files = Seq(
      ("data/G1_1k.csv", "G1"),
      ("data/G2_5k.csv", "G2"),
      ("data/G3_8k.csv", "G3"),
      ("data/G4_10k.csv", "G4")
    )

    var results = Seq.empty[(String, Long, Long, Double)]

    files.foreach { case (path, label) =>
      println(s"\n📎 Graphe $label ($path)")

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .toDF("source", "target")
        .withColumn("source", col("source").cast("long"))
        .withColumn("target", col("target").cast("long"))
        .na.drop()

      val nbEdges = df.count()
      val nbNodes = df.select("source").union(df.select("target")).distinct().count()

      val (components, duration) = ccfGraphX(df, spark)

      println(s"⏱️ Temps : ${"%.3f".format(duration)} sec")
      println("-" * 40)

      results = results :+ (label, nbNodes, nbEdges, duration)
    }

    val dfResults = results.toDF("Graphe", "Noeuds", "Arêtes", "Temps (s)")
    println("\n✅ Résumé des performances (GraphX) :")
    dfResults.show(truncate = false)

    println("\n🏁 Fin du programme CCF GraphX")
    spark.stop()
  }

  def ccfGraphX(df: DataFrame, spark: SparkSession): (DataFrame, Double) = {
    import spark.implicits._

    val startTime = System.nanoTime()

    val edgesRDD: RDD[Edge[Long]] = df.rdd.map(row => {
      val src = row.getAs[Long]("source")
      val dst = row.getAs[Long]("target")
      Edge(src, dst)
    })

    val graph = Graph.fromEdges(edgesRDD, 1L)
    val cc = graph.connectedComponents()
    val componentsDF = cc.vertices.toDF("node", "component")

    componentsDF.count() // Force l'exécution pour mesurer le temps
    val duration = (System.nanoTime() - startTime) / 1e9

    (componentsDF, duration)
  }
}
