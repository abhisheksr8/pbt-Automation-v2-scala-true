package io.prophecy.pipelines.automationpbt_v2truescala

import io.prophecy.libs._
import io.prophecy.pipelines.automationpbt_v2truescala.config._
import io.prophecy.pipelines.automationpbt_v2truescala.udfs.UDFs._
import io.prophecy.pipelines.automationpbt_v2truescala.udfs.PipelineInitCode._
import io.prophecy.pipelines.automationpbt_v2truescala.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_s3_source_dataset = s3_source_dataset(context)
    lookup_by_customer_id_and_email(context, df_s3_source_dataset)
    val df_concatenated_customer_info =
      concatenated_customer_info(context, df_s3_source_dataset)
    val df_script_1_identity =
      script_1_identity(context, df_concatenated_customer_info)
    val df_select_all_from_in0 =
      select_all_from_in0(context, df_s3_source_dataset)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AutomationPBT_v2-truescala")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/AutomationPBTNo_v2-truescala"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/AutomationPBTNo_v2-truescala"
    ) {
      apply(context)
    }
  }

}
