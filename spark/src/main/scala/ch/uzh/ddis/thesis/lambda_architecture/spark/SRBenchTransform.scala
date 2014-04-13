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
object SRBenchTransform {

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

    val ck = Map(("bill", 17 to 21),
      ("nevadastorm", 1 to 6),
      ("charley", 9 to 14),
      ("ike", 1 to 14),
      ("gustav", 25 to 31),
      ("katrina", 23 to 29),
      ("wilma", 17 to 22))

    for (hurricane <- hurricanes) {
      val file = new io.File("/var/srbench/" + hurricane)
      if (file.exists() && file.isDirectory) {
        for (i <- 'A' to 'Z'){
          if (i == 'C' || i == 'K'){
            for (j <- ck(hurricane)){
              val inputPath = "/var/srbench/" + hurricane + "/rdf/"+ i + "*_"+ j +".n3"
              val resPath = "/var/srbench/"+hurricane+"/res-num/" + i + "" + j
              sparkMagic(inputPath, resPath)
            }
          } else{
            val inputPath = "/var/srbench/" + hurricane + "/rdf/"+ i + "*.n3"
            val resPath = "/var/srbench/" + hurricane + "/res-num/" + i
            sparkMagic(inputPath, resPath)

          }
        }
      }
    }
  }

  def sparkMagic(inputPath: String, resultPath: String) {

    Runtime.getRuntime().exec("echo 3 > /proc/sys/vm/drop_caches");

    System.setProperty("spark.local.dir", "/tmp/spark/temp")
    System.setProperty("spark.executor.memory", "27g")

    val sc = new SparkContext("local[8]", "SRBench", System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))

    val data = sc.textFile(inputPath, 1)
    data.mapPartitions(x => convertRDFtoIterable(x))
      .repartition(20)
      .map(x => (x._1, x))
      .sortByKey()
      .map {
      x =>
        val str = new StringBuilder();
        str ++= (x._2._1);
        str ++= ",";
        str ++= x._2._5;
        str ++= ",";
        str ++= x._2._4;
        str ++= ",";
        str ++= x._2._3;
        str ++= ",";
        str ++= x._2._2;
        str ++= ",";
        str ++= x._2._6;

        str.toString
    }
      .coalesce(1)
      .saveAsTextFile(resultPath)

    Thread.sleep(5000)
    sc.stop()


    Runtime.getRuntime().exec("echo 3 > /proc/sys/vm/drop_caches");



  }


    def convertRDFtoIterable(list: Iterator[String]): Iterator[(String, String, String, String, String, String)] = {

      val str = new StringBuilder()
      list.foreach(s => str.append(s))
      val in = IOUtils.toInputStream(str.toString())


      val model = ModelFactory.createDefaultModel();
      model.read(in, null, "N3")

      val observations = model.getProperty(generatedObservation)

      val l = ListBuffer[(String, String, String, String, String, String)]()

      val it = model.listObjectsOfProperty(observations)
      while (it.hasNext) {
        val resStr = it.next()
        val res = model.getResource(resStr.toString)
        val propertyIt = res.listProperties()

        var propertyList = ListBuffer[String]()


        val timeSelector = new SimpleSelector(res, model.getProperty(samplingTime), null.asInstanceOf[RDFNode])
        val timeList = model.listStatements(timeSelector)
        if (timeList.hasNext) {
          val o = timeList.next().getObject.asResource()
          val time = o.asResource().getProperty(model.getProperty(owlTime + inXSDDateTime)).getString
          val newtime = time.replace("^^http://www.w3.org/2001/XMLSchema#dateTime", "")

          var timeSeconds: Long = 0
          try {
            val d = new DateTime(newtime)
            timeSeconds = d.toDate.getTime / 1000
          } catch{
            case e: Exception => {
              logger.error("date convert failed: " + e.getMessage)
              val sdf = new SimpleDateFormat(dateFormat)
              timeSeconds = sdf.parse(newtime).getTime / 1000
            }
          }

          propertyList += timeSeconds.toString
        }

        val resultSelector = new SimpleSelector(res, model.getProperty(result), null.asInstanceOf[RDFNode])
        val resultList = model.listStatements(resultSelector)
        if (resultList.hasNext) {
          val o = resultList.next().getObject.asResource()
          if (o.toString.contains("TruthData")) {
            val f = o.asResource().getProperty(model.getProperty(booleanValue)).getBoolean

            propertyList += "boolean"
            propertyList += f.toString

          } else {
            val uomValueUri = o.asResource().getProperty(model.getProperty(uom)).getObject.toString
            val uomValue = uomValueUri.replace("http://knoesis.wright.edu/ssw/ont/weather.owl#", "")
            //println(uomValue)
            propertyList += uomValue.toString

            val f = o.asResource().getProperty(model.getProperty(floatValue)).getFloat
            //println(f)
            propertyList += f.toString
          }

        }

        val observedPropertyUri = res.getProperty(model.getProperty(observedProperty)).getResource.toString
        val observedPropertyValue = observedPropertyUri.replace("http://knoesis.wright.edu/ssw/ont/weather.owl#_", "")
        //println(observedPropertyValue)
        propertyList += observedPropertyValue.toString

        val procedureUri = res.getProperty(model.getProperty(procedure)).getResource.toString
        val procedureValue = procedureUri.replace("http://knoesis.wright.edu/ssw/System_", "")
        //println(procedureValue)
        propertyList += procedureValue.toString

        val rdfTypeUri = res.getProperty(model.getProperty(rdfType)).getResource.toString
        val rdfTypeValue = rdfTypeUri.replace("http://knoesis.wright.edu/ssw/ont/weather.owl#", "")
        //println(rdfTypeValue)
        propertyList += rdfTypeValue.toString

        val t = (propertyList(0), propertyList(1), propertyList(2), propertyList(3), propertyList(4), propertyList(5))

        l += t

      }

      l.iterator

    }


  }
