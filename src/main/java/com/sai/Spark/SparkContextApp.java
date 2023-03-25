package com.sai.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class SparkContextApp {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkContextApp").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        System.out.println(sqlContext.sparkContext().appName());
    }

}
