import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.immutable.ListMap
import org.apache.spark.rdd.PairRDDFunctions
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.util.concurrent.TimeUnit

object MissedConnections {

  //check for Actual time 
  def checkActualTime(arrTime: String, deptTime: String): Boolean = {
    var arrMins: Int = 0
    var depMins: Int = 0
    arrMins = java.lang.Integer.parseInt(arrTime.substring(0, 2)) *
        60 +
        java.lang.Integer.parseInt(arrTime.substring(2))
      depMins = java.lang.Integer.parseInt(deptTime.substring(0, 2)) *
        60 +
        java.lang.Integer.parseInt(deptTime.substring(2))
   
    if ((depMins - arrMins) <= 30) return true
    false
  }
  //check for scheduled time
  def checkScheduledTime(arrTime: String, deptTime: String): Boolean = {
    var arrMins: Int = 0
    var depMins: Int = 0
    try {
      arrMins = java.lang.Integer.parseInt(arrTime.substring(0, 2)) *
        60 +
        java.lang.Integer.parseInt(arrTime.substring(2))
      depMins = java.lang.Integer.parseInt(deptTime.substring(0, 2)) *
        60 +
        java.lang.Integer.parseInt(deptTime.substring(2))
    } catch {
      case e: NumberFormatException => return false
    }
    if ((depMins >= arrMins + 30) && (depMins <= 360 + arrMins)) return true
    false
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("MissedConnections").
      setMaster("local")

    val sc = new SparkContext(conf)

    val formattedLine = sc.textFile("all/").map { _.replaceAll("\"", "").replaceAll(", ", "#") }.map { (_.split(",")) }
    val flightInfo = formattedLine.filter { _(0) != "YEAR" }

    //data about the origin of the flight
    val originDetails = flightInfo.map(
        
      //Key : carrier name,Year,Origin Id,date
      //Value : arrv time + sched arrv time+ date+ cancellation
        orgData => (orgData(6) + "," + orgData(0) + "," + orgData(11) + "," + orgData(5),

        Array(orgData(41), orgData(40), orgData(5), orgData(47))))

    //data about the destination of the flight 
    val destinationDetails = flightInfo.map(
      //Key : carrier name,Year,Destination Id,date
      //Value : dept time + sched dept time+ date+ cancellation
        destData => (destData(6) + "," + destData(0) + "," + destData(20) + "," + destData(5),
 
        Array(destData(30), destData(29), destData(5), destData(47))))

    //Join two RDDs
    val data = originDetails.join(destinationDetails)

    val validRecords = data.map(
      rec => {
        val (key, value) = rec;
        val (origin, destination) = value;

        val arrvTime = (origin(0))
        val schedArrvTime = (origin(1))
        val oDate = (origin(2))
        val originCancellation = (origin(3))

        val deptime = (destination(0))
        val schedDeptTime = (destination(1))
        val dDate = (destination(2))
        val destCancellation = (destination(3))

        if (!deptime.isEmpty && !arrvTime.isEmpty && !dDate.isEmpty && !oDate.isEmpty) {

          if (checkScheduledTime(schedArrvTime, schedDeptTime)) {
            if (checkActualTime(arrvTime, deptime) || originCancellation.toString.equals("1")) {
              //totalConnection = 1 , missedConnection = 1
              (key, (1, 1))
            } else {
              //totalConnection = 1 , missedConnection = 0
              (key, (1, 0))
            }
          } else {
            //totalConnection = 0 , missedConnection = 0 
            (key, (0, 0))
          }
        } else {
          //totalConnection = 0 , missedConnection = 0
          (key, (0, 0))
        }
      })
		  val textCollect = validRecords.reduceByKey((a1,a2) => (a1._1 + a2._1, a1._2 + a2._2)).collect()

		  val outputData = textCollect.map(value => {
		 	val (k,v) = value;
		 	val (a1,a2) = v;
      val d = k.split(",")
      val carrierName = (d(0))
      val year = (d(1))
      carrierName.toString+","+year.toString+ "," + a1.toString + "," + a2.toString
                        
		 	})
		
		var resultSet = sc.parallelize(outputData)
    resultSet.saveAsTextFile("output1")

    // Shut down Spark
    sc.stop()

  }
}