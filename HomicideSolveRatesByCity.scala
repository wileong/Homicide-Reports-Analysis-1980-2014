// Finds the % of crime solved by each city.
import org.apache.spark.{ SparkConf, SparkContext }

object AgencySolveRate {
  def main(args: Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("AgencySolveRate").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //2 is the Agency name and 10 is whether or not the crime was solved ("yes" or "no")
    val lines = sc.textFile("homicideReports1980To2014.csv").
      map(line => (
        // line.split(",")(1).trim(),
        line.split(",")(2).trim(),
        line.split(",")(10).trim()))

    val solved = lines.mapValues( x =>
      if (x.equals("Yes"))
        (1, 1)
      else
        (0, 1)
    ).reduceByKey( (x, y) =>
      (x._1 + y._1, x._2 + y._2)
    ).mapValues(
      { case(yes, total) => yes*1.0/total }
    ).sortBy(_._2 , false)
    
    solved.foreach( println(_) )
    
    println("Calculating percentages of homicide cases solved by cities in the U.S. ... Done.\n")
  }
}
