import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Example2 {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // load initial RDD
    val rdd = sc.parallelize(List(
      MyTuple(21, Person("James", "Jones", 21, "CO")),
      MyTuple(44, Person("Basil", "Brush" ,44, "AK"))
    ))

    // convert to DataFrame (RDD + Schema)
    val dataFrame = sqlContext.createDataFrame(rdd)

    // write the data to parquet
    dataFrame.write.parquet("./parquet_test")

  }

}

case class MyTuple(age: Int, person: Person)

case class Person(first: String, last: String, age: Int, state: String)

