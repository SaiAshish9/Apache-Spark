package com.sai.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

public class SparkHiveApp {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkHiveApp").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        HiveContext hiveContext = new HiveContext(sparkContext);
        System.out.println(hiveContext.sparkContext().appName());
    }

}
