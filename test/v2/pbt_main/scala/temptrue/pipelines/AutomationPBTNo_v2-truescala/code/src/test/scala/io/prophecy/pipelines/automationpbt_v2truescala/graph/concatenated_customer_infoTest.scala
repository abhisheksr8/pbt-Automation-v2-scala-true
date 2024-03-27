package io.prophecy.pipelines.automationpbt_v2truescala.graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.pipelines.automationpbt_v2truescala.config._
import io.prophecy.libs.registerAllUDFs
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.math.BigDecimal

@RunWith(classOf[JUnitRunner])
class concatenated_customer_infoTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test ") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/automationpbt_v2truescala/graph/concatenated_customer_info/in/schema.json",
      "/data/io/prophecy/pipelines/automationpbt_v2truescala/graph/concatenated_customer_info/in/data/unit_test_.json",
      "in"
    )
    val dfOut = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/automationpbt_v2truescala/graph/concatenated_customer_info/out/schema.json",
      "/data/io/prophecy/pipelines/automationpbt_v2truescala/graph/concatenated_customer_info/out/data/unit_test_.json",
      "out"
    )

    val dfOutComputed = io.prophecy.pipelines.automationpbt_v2truescala.graph
      .concatenated_customer_info(context, dfIn)
    val res = assertDFEquals(
      dfOut.select("phone", "email", "customer_id", "first_name", "last_name"),
      dfOutComputed.select("phone",
                           "email",
                           "customer_id",
                           "first_name",
                           "last_name"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
  }

  override def beforeAll() = {
    super.beforeAll()
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerAllUDFs(spark)

    val fabricName = System.getProperty("fabric", "default")
    val confFilePath = Paths
      .get(getClass.getResource(s"/config/${fabricName}.json").toURI)
      .toString

    val config =
      ConfigurationFactoryImpl.getConfig(Array("--confFile", confFilePath))

    context = Context(spark, config)
  }

}
