package com.github.mssalvador.matrixInv

import org.apache.spark.SparkContext
//import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, SingularValueDecomposition, Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object MatrixInv {
  def loadData(sc: SparkContext, jdf: DataFrame, column: String): DataFrame = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

//    jdf.select(column).show()
//    jdf.select(column).printSchema()

    val matData = jdf.rdd.map(line => line.getAs[DenseVector](column).toArray) // Lav RDD'en med Array om til en RDD med dense Vector
    val rowMatData = new RowMatrix(matData.map(line => Vectors.dense(line)))
    val outData = computeInvese(rowMatData) // Beregner den inverse af matricen
    matrixToRDD(sc, outData).zipWithIndex().toDF() // Skriver matrix tilbage til Python som Dataframe med Array
  }

  def ingestData(sc: SparkContext, rdd: RDD[Vector]): RDD[Vector] = {

    val rowMatrix = new RowMatrix(rdd)
    val inversion = computeInvese(rowMatrix)
    matrixToRDD(sc, inversion).map(row => new DenseVector(row))
  }

  def computeInvese(X: RowMatrix): DenseMatrix = {
    val nCoef = X.numCols().toInt
    val svd = X.computeSVD(nCoef, computeU = true)
    if (svd.s.size < nCoef) {
      sys.error("RowMatrix. computeInverse called on singluar matrix.")
    }

    // Diagonalen er bare at invertere sig selv.
    val invS = DenseMatrix.diag(new DenseVector(svd.s.toArray.map(x => math.pow(x, -1))))

    // U skal laves om til en RowMatrix
    val U = new DenseMatrix(svd.U.numRows().toInt, svd.U.numCols().toInt, svd.U.rows.collect.flatMap(x => x.toArray))

    val V = svd.V
    (V.multiply(invS)).multiply(U)
  }

  def matrixToRDD(sc: SparkContext ,m: DenseMatrix): RDD[Array[Double]] = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose
    val vectors = rows.map(row => row.toArray)
    sc.parallelize(vectors)
  }
}
