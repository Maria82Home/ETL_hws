/*
chcp 65001 && spark-shell -i \GeekBrains\Data_Engineer\ETL\hws\hw1\hw1.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("C:/GeekBrains/Data_Engineer/ETL/hws/hw1/hw1.xlsx")
		df1.show()
		df1.withColumn("id",monotonicallyIncreasingId)
		.orderBy("id").drop("id","Name","Job", "Home_city")
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketlhw1c")
        .mode("overwrite").save()
        df1.select("Employee_ID","Name").distinct()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketlhw1d")
        .mode("overwrite").save()
        df1.select("Job_Code","Job").distinct()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketlhw1e")
        .mode("overwrite").save()
        df1.select("City_code", "Home_city").distinct()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=root")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketlhw1f")
        .mode("overwrite").save()
	println("task 1")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)