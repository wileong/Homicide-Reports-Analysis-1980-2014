// Find the top 5 states with the most homicides from the years [1980,2014]
import org.apache.spark.{SparkConf, SparkContext}

object top5 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("top5states").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val pops = sc.textFile("populations.csv").map( line =>
      (line.split(",")(0).toLowerCase, line.split(",")(1).toLowerCase, line.split(",")(2)))
    val homs = sc.textFile("homicideData.csv").map( line =>
      (line.split(",")(4).toLowerCase, line.split(",")(5).toLowerCase, line.split(",")(6)))

    /*
     * pops contains [City, State, Population (2014)]
     * homs contains [City, State, Year]
     */

    val homs2014s = homs.filter{ case(k, v1, v2) => v2 > "1979" } // filter for 1980-2014

    // homsMap contains [ ((city,state),1) ]
    val homsMap = homs2014s.map{ case(k, v1, v2) => (k.concat(",").concat(v1), 1) }
    val homsByState = homsMap.reduceByKey( (x,y) => x+y )
    val top5Homs = homsByState.map{ case(k,v) => (v.toDouble/9,k) }.sortByKey(false).take(5)
    println("Top 5 Non-Standardized:")
    top5Homs.foreach{ case(k,v) => println(v + "," + k) }

    /*
     * Standardize by city population
     */

    // popsMap contains [ ((city,state),population) ]
    val popsMap = pops.map{ case(k, v1, v2) => (k.concat(",").concat(v1), v2.toInt) }
    val joinedVals = homsByState.join(popsMap)

    // standardized value is (total homicides/total population)
    val standardVals = joinedVals.map{ case(k, (v1, v2)) => (k, ((v1.toDouble/9)/v2, v1.toDouble/9, v2)) }
    val top5stVals = standardVals.map{ case(k, (v1, v2, v3)) => (v1,(k,v2,v3)) }.sortByKey(false).take(5)
    println("Top 5 Standardized:")
    top5stVals.foreach{ case(k,(v1,v2,v3)) => println(v1 + "," + v2 + "," + v3) }
  }
}