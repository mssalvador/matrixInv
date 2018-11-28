package com.github.mssalvador.matrixInv

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("matrix_inverse")
      .getOrCreate()
  }

}
