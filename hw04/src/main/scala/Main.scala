package org.apache.spark.ml.linreg

import breeze.linalg.{*, DenseMatrix, DenseVector}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, Pipeline}
import org.apache.spark.mllib
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import org.apache.log4j.Logger
import org.apache.log4j.Level

trait Params
  extends HasInputCol
    with HasOutputCol
    with HasLabelCol
    with HasFitIntercept {

  val numIter = 1000
  val eps = 1e-4

  def getNumIter: Int = numIter
  def getEps: Double = eps

  def setInputCol(inputCol: String): this.type = set(this.inputCol, inputCol)
  def setOutputCol(outputCol: String): this.type = set(this.outputCol, outputCol)
  def setLabelCol(labelCol: String): this.type = set(this.labelCol, labelCol)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, getInputCol, new VectorUDT())

    if (schema.fieldNames.contains(getOrDefault(outputCol))) {
      SchemaUtils.checkColumnType(schema, getOutputCol, new VectorUDT())
      return schema
    }
    SchemaUtils.appendColumn(schema, schema(getInputCol).copy(name = getOutputCol))
  }

}

class LinearRegression(override val uid: String)
  extends Estimator[LinearRegressionModel]
    with Params
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("LinearRegression"))

  override def fit(dataset: Dataset[_]): LinearRegressionModel = {

    implicit val encoder: Encoder[Vector] = ExpressionEncoder()
    val vectors: Dataset[Row] = dataset.select(dataset(getOrDefault(inputCol)), dataset(getOrDefault(labelCol)))

    val dim: Int = AttributeGroup.fromStructField(dataset.schema(getOrDefault(inputCol))).numAttributes.getOrElse(vectors.first().getAs[Vector](0).size)
    var weights = Vectors.dense(Array.fill(dim) {
      scala.util.Random.nextDouble
    })
    var bias = scala.util.Random.nextDouble

    def getSum(summarizer: MultivariateOnlineSummarizer, vector: Row): MultivariateOnlineSummarizer = {
      val x = vector.getAs[Vector](0)
      val y = vector.getDouble(1)
      var err = y - x.dot(weights)

      if (getOrDefault(fitIntercept)) {
        err = err - bias
      }
      var grad = x.asBreeze * err
      if (getOrDefault(fitIntercept)) {
        grad = breeze.linalg.DenseVector.vertcat(grad.toDenseVector, breeze.linalg.DenseVector(err))
      }
      summarizer.add(mllib.linalg.Vectors.fromBreeze(grad))
    }

    def calcSum(): Unit = {
      for (_ <- 0 to getNumIter) {
        val summary = vectors.rdd.mapPartitions((data: Iterator[Row]) => {
          val result = data.foldLeft(new MultivariateOnlineSummarizer()) {
            getSum
          }
          Iterator(result)
        }).reduce(_ merge _)
        weights = Vectors.fromBreeze(weights.asBreeze + summary.mean.asBreeze(0 to dim))
        if (getOrDefault(fitIntercept)) bias = bias + summary.mean.asBreeze(dim)
        if (summary.mean.asBreeze.reduce(_ + _).abs < getEps) return
      }
    }

    calcSum()
    copyValues(new LinearRegressionModel(weights, bias)).setParent(this)

  }

  override def copy(extra: ParamMap): Estimator[LinearRegressionModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

}

class LinearRegressionModel private (override val uid: String, val weights: org.apache.spark.ml.linalg.DenseVector, val bias: Double)
  extends Model[LinearRegressionModel]
    with Params
    with MLWritable {

  def this(weights: Vector, bias: Double) = this(Identifiable.randomUID("myLinearRegressionModel"), weights.toDense, bias)

  override def copy(extra: ParamMap): LinearRegressionModel = copyValues(new LinearRegressionModel(weights, bias), extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val w = weights.asBreeze
    val transformUdf = {

      if (getOrDefault(fitIntercept)) {
        dataset.sqlContext.udf.register(uid + "_t",
          (x: Vector) => {
            x.asBreeze.dot(w) + bias
          })
      } else {
        dataset.sqlContext.udf.register(uid + "_t",
          (x: Vector) => {
            x.asBreeze.dot(w)
          })
      }
    }

    dataset.withColumn(getOrDefault(outputCol), transformUdf(dataset(getOrDefault(inputCol))))
  }

  override def write: MLWriter = new DefaultParamsWriter(this) {
    override protected def saveImpl(path: String): Unit = {
      super.saveImpl(path)
    }
  }

}

object Main {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("LinearRegression")
      .getOrCreate()

    import spark.implicits._

    val N = 100000
    val a1 = 1.5
    val a2 = 0.3
    val a3 = -0.7
    val sc = 5.0

    val X = DenseMatrix.rand(N, 3)
    val epsVector = sc * DenseVector.rand(N)
    val y = epsVector + X * DenseVector(a1, a2, a3)
    val data = DenseMatrix.horzcat(X, y.asDenseMatrix.t)

    val df = data(*, ::).iterator.map(x => (x(0), x(1), x(2), x(3))).toSeq.toDF("x1", "x2", "x3", "y")
    val pipeline = new Pipeline()
    val vAssembler = new VectorAssembler().setInputCols(Array("x1", "x2", "x3")).setOutputCol("X")
    val LR = new LinearRegression().setInputCol("X").setLabelCol("y").setOutputCol("w")
    val stages = Array(vAssembler, LR)
    pipeline.setStages(stages)
    val model = pipeline.fit(df)
    val w = model.stages.last.asInstanceOf[LinearRegressionModel].weights
    println(w)
  }
}
