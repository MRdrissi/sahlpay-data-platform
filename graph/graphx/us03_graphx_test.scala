// =========================================================
// US-03 / SBD-60 - Validation de GraphX en Scala
// Execution : voir docs/epics/EPIC-01-infrastructure.md
// =========================================================
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

println(s"Spark ${spark.version} | Scala ${scala.util.Properties.versionNumberString}")

// Sommets : (VertexId, (identifiant metier, type))
val vertices: RDD[(VertexId, (String, String))] = sc.parallelize(Seq(
  (1L, ("W-1", "compte")),
  (2L, ("W-2", "compte")),
  (3L, ("W-3", "compte")),
  (4L, ("D-1", "appareil")),
  (5L, ("W-4", "compte")),
  (6L, ("W-5", "compte"))
))

// Aretes : (source, destination, relation)
val edges: RDD[Edge[String]] = sc.parallelize(Seq(
  Edge(1L, 4L, "USES_DEVICE"),
  Edge(2L, 4L, "USES_DEVICE"),
  Edge(3L, 4L, "USES_DEVICE"),
  Edge(1L, 2L, "TRANSFERS_TO"),
  Edge(5L, 6L, "TRANSFERS_TO")
))

val graph = Graph(vertices, edges)
println(s"Sommets : ${graph.numVertices} | Aretes : ${graph.numEdges}")

println("--- Degres (entrants + sortants) ---")
graph.degrees.join(vertices)
  .map { case (_, (deg, (name, kind))) => (name, kind, deg) }
  .collect().sortBy(-_._3)
  .foreach { case (n, k, d) => println(f"$n%-4s $k%-9s degre = $d") }

println("--- Composantes connexes ---")
graph.connectedComponents().vertices.join(vertices)
  .map { case (_, (cc, (name, _))) => (cc, name) }
  .groupByKey().collect()
  .foreach { case (cc, names) => println(s"composante $cc : ${names.toSeq.sorted.mkString(", ")}") }

println("--- PageRank ---")
graph.pageRank(0.0001).vertices.join(vertices)
  .map { case (_, (rank, (name, _))) => (name, rank) }
  .collect().sortBy(-_._2)
  .foreach { case (n, r) => println(f"$n%-4s $r%.3f") }
