package org.halim;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("PlanesWithMostIncidents")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> incidentsDF = spark.read()
                .format("csv")
                .option("header", true)
                .load("incidents.csv");

        incidentsDF.createOrReplaceTempView("incidents");

        String query = "SELECT no_avion, COUNT(*) AS incident_count " +
                "FROM incidents " +
                "GROUP BY no_avion " +
                "ORDER BY incident_count DESC " +
                "LIMIT 1";

        Dataset<Row> result = spark.sql(query);
        result.show();
        spark.close();

    }
}