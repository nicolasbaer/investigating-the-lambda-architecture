package ch.uzh.ddis.thesis.lambda_architecture.spark

import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.log4j.LogManager
import java.text.SimpleDateFormat
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.SimpleSelector
import com.hp.hpl.jena.rdf.model.RDFNode
import scala.collection.mutable.ListBuffer
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToOrderedRDDFunctions
import java.io
import org.joda.time.DateTime

/**
 *
 *
 * @author Nicolas Baer <nicolas.baer@gmail.com>
 */
object SRBenchFat {

  private val logger = LogManager.getLogger(SRBench.getClass.getName)

  private val omOwl = "http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#"
  private val owlTime = "http://www.w3.org/2006/time#"
  private val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

  private val rdfType = rdf + "type"

  private val inXSDDateTime = "inXSDDateTime"

  private val generatedObservation = omOwl + "generatedObservation"
  private val samplingTime = omOwl + "samplingTime";
  private val result = omOwl + "result"
  private val procedure = omOwl + "procedure"
  private val observedProperty = omOwl + "observedProperty"

  private val uom = omOwl + "uom"
  private val floatValue = omOwl + "floatValue"
  private val booleanValue = omOwl + "booleanValue"

  private val dateFormat = "yyyy-MM-dd'T'HH:mm:ss"


  def main(args: Array[String]) {
    val hurricanes = List("bill", "nevadastorm", "charley", "ike", "gustav", "katrina", "wilma")

    for (hurricane <- hurricanes) {
      val file = new io.File("/var/srbench/" + hurricane)
      if (file.exists() && file.isDirectory) {
            val inputPath = "/var/srbench/" + hurricane + "/res-num/*/part-00000"
            val resPath = "/var/srbench/" + hurricane + "/res-fat/"
            sparkMagic(inputPath, resPath)
      }
    }
  }

  def sparkMagic(inputPath: String, resultPath: String) {

    Runtime.getRuntime().exec("echo 3 > /proc/sys/vm/drop_caches");

    System.setProperty("spark.local.dir", "/tmp/spark/temp")
    System.setProperty("spark.executor.memory", "27g")

    val sc = new SparkContext("local[8]", "SRBench", System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))

    val data = sc.textFile(inputPath, 1)
    data.map{x => val split = x.split(","); (split(0), x)}
      .sortByKey()
      .map {
        x => x._2
      }
      .coalesce(1)
      .saveAsTextFile(resultPath)

    Thread.sleep(5000)
    sc.stop()


    Runtime.getRuntime().exec("echo 3 > /proc/sys/vm/drop_caches");

  }

}
