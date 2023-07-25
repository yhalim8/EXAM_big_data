package org.halim;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Calendar;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Spark {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkConf sparkConf = new SparkConf().setAppName("ContinuousMonthlyIncidentAnalysis").setMaster("local[*]");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> incidentsDF = spark.read().format("csv").option("header", true).load("incidents.csv");

        incidentsDF = incidentsDF.withColumn("date", to_date(col("date"), "yyyy-MM-dd"));

        int currentYear = Calendar.getInstance().get(Calendar.YEAR);
        incidentsDF = incidentsDF.filter(year(col("date")).equalTo(currentYear));

        incidentsDF = incidentsDF.withColumn("month", month(col("date")));

        incidentsDF.createOrReplaceTempView("incidents");

        Dataset<Row> result = spark.sql("SELECT month, COUNT(*) AS incident_count " +
                "FROM incidents " +
                "GROUP BY month " +
                "ORDER BY incident_count DESC").show(2);

        spark.close();
    }
}
