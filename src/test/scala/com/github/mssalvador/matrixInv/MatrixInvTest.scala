package com.github.mssalvador.matrixInv

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalactic.TolerantNumerics
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession

class MatrixInvTest extends FunSuite{

  val epsilon = 1e-2f

  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  // Create initial test Is I^-1 = I???
  val denseData = Seq(
    Vectors.dense(1.0,0.0),
    Vectors.dense(0.0, 1.0)
  )
  val denseMatrix = new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 1.0))
  val spConfig = (new SparkConf).setMaster("local").setAppName("SparkSVDDemo")
  val sc = new SparkContext(spConfig)


  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  val mat = new RowMatrix(sc.parallelize(denseData, 2))

  test("MatrixInv.computeInveseIdentity") {
    assert(MatrixInv.computeInvese(mat) == denseMatrix)
  }

  // Take a larger matrix 5 x 5

  val largeDenseData = Seq(
    Vectors.dense(5.0, 5.0, 5.0, 5.0, 2.0),
    Vectors.dense(7.0, 2.0, 6.0, 2.0, 8.0),
    Vectors.dense(9.0, 2.0, 6.0, 3.0, 8.0),
    Vectors.dense(5.0, 1.0, 3.0, 7.0, 8.0),
    Vectors.dense(6.0, 7.0, 8.0, 3.0, 2.0)
  )

  val largeDenseMatrix = Array(
    1.20,
    7.46,
    -7.32,
    -2.41,
    3.17,
    0.62,
    6.39,
    -5.97,
    -2.25,
    3.023,
    -0.09,
    -3.65,
    3.25,
    1.186,
    -1.74,
    -0.60,
    -3.23,
    3.16,
    1.20,
    -1.33,
    -0.93,
    -5.51,
    5.55,
    1.86,
    -2.44)

  val largeMat = new RowMatrix(sc.parallelize(largeDenseData, numSlices = 2))

  for ((x, i) <- MatrixInv.computeInvese(largeMat).toArray.zipWithIndex) {
    test("MatrixInv.computeInveseLarge"+i) {
      assert(x === largeDenseMatrix(i))

    }
  }

  val arrayMat = Seq(Array(5,4,2), Array(5,6,7), Array(1,2,5))

  val df = sc.parallelize(arrayMat).toDF()
  df.show()

  val rdd = MatrixInv.loadData(sc, df, "value")
//  rdd.take(3).foreach(println)


  test("Testing computations") {
    import math._
    assert(sin(Pi/4) === sqrt(0.5))
    assert(sin(Pi) === 0.0001)
  }

}
