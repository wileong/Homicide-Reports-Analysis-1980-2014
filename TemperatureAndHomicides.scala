import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import java.io._

object HotVsCold_And_YoungestPerpetrators {

  def main(args: Array[String]): Unit = {

    //assumes a directory "C:\winutils\bin" has already been created on a windows fs
    System.setProperty("hadoop.home.dir", "c:/winutils/")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // .setMaster("local[4]")
    //parameter to setMaster tells us how to distribute
    // the data, e.g., 4 partitions on the localhost
    // don't use setMaster if running on cluster
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]") // .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val hotVsColdDataLines = sc.textFile("homicideReports1980To2014.csv") //put input file above src dir in an intellij project

    // an array of tuples. Format: ( (Year, Month), Record ID)
    // note: must always return a tuple so array stays an array of tuples and not an object
    // 0 are distinct Record ID's of each homicide, 6 is year, 7 is month
    val hotVsColdData = hotVsColdDataLines.map(_.split(",")).map(
      x=> if (!x(0).contentEquals("Record ID")) {
        x(7) match {
          case "January" => ( (x(6).toInt, 1), x(0).toInt )
          case "February" => (  (x(6).toInt, 2), x(0).toInt )
          case "March" => (  (x(6).toInt, 3), x(0).toInt )
          case "April" => (  (x(6).toInt, 4), x(0).toInt )
          case "May" => (  (x(6).toInt, 5), x(0).toInt )
          case "June" => (  (x(6).toInt, 6), x(0).toInt )
          case "July" => (  (x(6).toInt, 7), x(0).toInt )
          case "August" => (  (x(6).toInt, 8), x(0).toInt )
          case "September" => (  (x(6).toInt, 9), x(0).toInt )
          case "October" => (  (x(6).toInt, 10), x(0).toInt )
          case "November" => (  (x(6).toInt, 11), x(0).toInt )
          case "December" => (  (x(6).toInt, 12), x(0).toInt )

        }

      } else {
        (None, None)
      }

    ).filter( _ != (None, None))

    // can't call sortByKey(); pattern matching needed to access key, which is a tuple,
    // and the elements inside key tuples are needed.
    val hotVsColdGroupedByYearAndMonth = hotVsColdData.groupByKey().sortBy(t=>t match {
      case ((k1:Int, v1:Int), v2) => (k1, v1)
    }).map(
      t=> t match {
        case ((k1, v1), v2) => ((k1, v1), v2.toList.length) //((year, month), number of homicides)
      }
    ).collect()

    val out = new PrintWriter(new File("numHomicidesHotVsColdMonthsByYear.csv"))

    var coldMonths: Array[Int] = Array(1,2,3,10,11,12)
    var hotMonths: Array[Int] = Array(4,5,6,7,8,9)

    hotVsColdGroupedByYearAndMonth.foreach(x=> x match {
      case ( (k1, v1), v2 ) =>
        out.write(v1 + "/" + k1 + "," + v2 + "\n") //output to csv: month/year, Number of Homicides

    })

    out.close()

    println("Finding Number of Homicides in Colder Vs Hotter Months ... Done.\n")

    // an array of tuples. Format: ( perpetrator age , city, state,
    // victim sex, victim age, perpetrator sex, recordId, Relationship to Victim, Weapon Used)
    val youngPerpetratorData = hotVsColdDataLines.map(_.split(",")).map(
      x=> if (!x(0).contentEquals("Record ID") ) {
        try {
          (x(16).toInt, (x(4), x(5), x(11), x(12), x(15), x(0), x(19), x(20) )) //9 elts
        } catch {
          case e: Exception => (None, None)
        }

      } else {
        (None, None)
      }

    ).filter( t=> t != (None, None) ).map(t=> t match {
      case (k:Int, (v1, v2, v3, v4, v5, v6, v7, v8)) =>
        (k.toInt, (v1, v2, v3, v4, v5, v6, v7, v8))
    }).sortByKey( false).filter( _._1 < 10 ).filter( _._1 > 0 ).collect()

    val out2 = new PrintWriter(new File("YoungestPerpetrators.csv"))

    youngPerpetratorData.foreach(x=> x match {
      case (k:Int, (v1, v2, v3, v4, v5, v6, v7, v8)) =>
        out2.write(v6 + "," + v1 + ","
          + v2 + "," + v3 + "," + v4
          + "," + v5 + "," + k + "," + v7 + ","
          + v8 + "\n") //output to csv: recordId, .... , Perpetrator Age, ...

    })

    out2.close()

    println("Finding perpetrators that were younger than 10 years old  ... Done.\n")

    
  }

}
