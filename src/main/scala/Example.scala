    package com.theotherandygrove

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{Row, SQLContext}
    import org.apache.spark.{SparkConf, SparkContext}

    object Example {

      def main(arg: Array[String]): Unit = {

        val conf = new SparkConf()
          .setAppName("Example")
          .setMaster("local[*]")

        val sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)

        val schema = StructType(List(
          StructField("person_id", DataTypes.IntegerType, true),
          StructField("person", StructType(List(
            StructField("first", DataTypes.StringType),
            StructField("last", DataTypes.StringType),
            StructField("age", DataTypes.IntegerType),
            StructField("state", DataTypes.StringType)
          )), true)))

        // load initial RDD
        val rdd = sc.parallelize(List(
          Person("James", "Jones", 21, "CO"),
          Person("Basil", "Brush" ,44, "AK")
        ))

        // convert to RDD[Row]
        val rowRdd = rdd.map(person => Row(person.age, person))

        // convert to DataFrame (RDD + Schema)
        val dataFrame = sqlContext.createDataFrame(rowRdd, schema)

        // write the data to parquet
        dataFrame.write.parquet("./parquet_test")

/*
        // register as a table
        dataFrame.registerTempTable("person")

        // selecting the whole object works fine
        println("Example 1: Select whole object");
        val results = sqlContext.sql("SELECT person FROM person")
        val people = results.collect
        people.map(row => println(row))

        // using functions to extract values works, but does not seem ideal
        println("Example 2: Select attribute from object using UDF");
        sqlContext.udf.register("getFirstName", (p: Person) => p.first)
        val results2 = sqlContext.sql("SELECT getFirstName(person) FROM person")
        val people2 = results2.collect
        people2.map(row => println(row))
        
        // what I really want to do, is this... but it fails with "Can't extract value from person#1"
        println("Example 3: Select attribute from object natively");
        val results3 = sqlContext.sql("SELECT person.firstName FROM person")
        val people3 = results3.collect
        people3.map(row => println(row))
*/

      }

    }


case class Person(first: String, last: String, age: Int, state: String)

