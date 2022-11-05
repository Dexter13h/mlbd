package ru.linreg

import breeze.linalg.{*, DenseMatrix, DenseVector, csvread, csvwrite, inv, norm, sum}
import breeze.stats.mean
import java.io.File

class LinearRegression() {
  private var W: DenseVector[Double] = DenseVector()
  private var Ready: Boolean = false

  def Fit(X: DenseMatrix[Double], Y: DenseVector[Double]): Double = {
    var XCopy: DenseMatrix[Double] = X.copy
    var Score: Double = 0
    if (!this.Ready) {
      XCopy = XCopy(::, *).map(Col => Col / norm(Col))
      this.W = inv(XCopy.t * XCopy) * XCopy.t * Y
      val YPred = XCopy.toDenseMatrix * this.W
      val YTrue = Y.toDenseVector
      Score = this.calcRSquared(YTrue, YPred)
      this.Ready = true
      Score
    } else {
      Score
    }
  }

  def Predict(X: DenseMatrix[Double]): DenseVector[Double] = {
    if (!this.Ready) {
      println("Call Fit() first then Predict().")
    }
    X(::, *).map(Col => Col / norm(Col)) * this.W
  }

  def calcRSquared(YTrue: DenseVector[Double],
                  YPred: DenseVector[Double]): Double = {
    val Res = sum((YTrue - YPred) * (YTrue - YPred))
    val MeanT = mean(YTrue)
    val Total = sum((YTrue - MeanT) * (YTrue - MeanT))
    1 - Res / Total
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val TrainF = args(0)
    val TestF = args(1)
    val PredF = args(2)
    var Train = csvread(new File(TrainF), ';', skipLines = 1)
    var Test = csvread(new File(TestF), ';', skipLines = 1)
    val LinearRegression = new LinearRegression()
    val Score = LinearRegression.Fit(X = Train(::, 1 to -1), Y = Train(::, 0))
    println(s"Score (R squared value): $Score")
    val Predicts = LinearRegression.Predict(X = Test(::, 1 to -1))
    csvwrite(new File(PredF), Predicts.asDenseMatrix.t)
  }
}