package com.sai.Spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCsv {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("READ_CSV").getOrCreate();
        Dataset<Row> dataset = sparkSession.sqlContext().read().format("com.databricks.spark.csv")
                        .option("header", true)
                        .load("src/main/resources/sample.csv");
        dataset.show(true);
    }
}
