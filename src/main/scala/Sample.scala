import georyetti.regex.mleap.model.RegexExtractModel
import ml.combust.mleap.spark.SimpleSparkSerializer
import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.ml.mleap.transformer.RegexExtract
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Employee(id: Int,
                    name: String,
                    phoneNumber: String,
                    dateJoined: String)

object Sample {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("sample-app")
    .getOrCreate()

  import spark.implicits._

  def createSampleData: DataFrame = {

    val data = Seq(
      Employee(1, "Smith, Mr. John", "(541) 471 3918","20-02-2019"),
      Employee(2, "Davis, Ms. Nicole", "(603)281-0308", "15/07/2020"),
      Employee(3, "Robinson, Mrs. Rebecca", "(814)-462-8074", "14.09.2021"),
      Employee(4, "Armstrong, Dr. Sam", "9704443106", "13.05/2018")
    )

    data.toDF
  }

  def buildPipeline(dataFrame: DataFrame, transformations: Array[Transformer]): PipelineModel = {
    val pipeline = new Pipeline().setStages(transformations)
    pipeline.fit(dataFrame)
  }

  def transformData(dataFrame: DataFrame, pipeline: PipelineModel): DataFrame = {
    pipeline.transform(dataFrame)
  }

  def buildNameTransformers: Seq[RegexExtract] = {
    val namePattern = "(\\w+),\\s(Mr|Ms|Mrs|Dr).?\\s(\\w+)"
    val surnameExtractor = new RegexExtract(
      uid = "surname_regex_extract",
      model = RegexExtractModel(regexString = namePattern, idx = 1)
    )
      .setInputCol("name")
      .setOutputCol("surname")

    val titleExtractor = new RegexExtract(
      uid = "title_regex_extract",
      model = RegexExtractModel(regexString = namePattern, idx = 2)
    )
      .setInputCol("name")
      .setOutputCol("title")

    val forenameExtractor = new RegexExtract(
      uid = "forename_regex_extract",
      model = RegexExtractModel(regexString = namePattern, idx = 3)
    )
      .setInputCol("name")
      .setOutputCol("forename")

    Seq(titleExtractor, forenameExtractor, surnameExtractor)
  }

  def buildPhoneNumberTransformers: Seq[RegexExtract] = {
    val phoneNumberPattern = ".?(\\d{3}).*(\\d{3}).*(\\d{4})"
    val phoneAreaCodeExtractor = new RegexExtract(
      uid = "area_code_regex_extract",
      model = RegexExtractModel(regexString = phoneNumberPattern, idx = 1)
    )
      .setInputCol("phoneNumber")
      .setOutputCol("area_code")

    val phoneExchangeExtractor = new RegexExtract(
      uid = "exchange_regex_extract",
      model = RegexExtractModel(regexString = phoneNumberPattern, idx = 2)
    )
      .setInputCol("phoneNumber")
      .setOutputCol("exchange")

    val phoneLineNumberExtractor = new RegexExtract(
      uid = "line_number_regex_extract",
      model = RegexExtractModel(regexString = phoneNumberPattern, idx = 3)
    )
      .setInputCol("phoneNumber")
      .setOutputCol("line_number")

    Seq(phoneAreaCodeExtractor, phoneExchangeExtractor, phoneLineNumberExtractor)
  }

  def buildDateJoinedTransformers: Seq[RegexExtract] = {
    val dateJoinedPattern = "(\\d{2}).(\\d{2}).(\\d{4})"
    val dayExtractor = new RegexExtract(
      uid = "day_regex_extract",
      model = RegexExtractModel(regexString = dateJoinedPattern, idx = 1)
    )
      .setInputCol("dateJoined")
      .setOutputCol("day")

    val monthExtractor = new RegexExtract(
      uid = "month_regex_extract",
      model = RegexExtractModel(regexString = dateJoinedPattern, idx = 2))
      .setInputCol("dateJoined")
      .setOutputCol("month")

    val yearExtractor = new RegexExtract(
      uid = "year_regex_extract",
      model = RegexExtractModel(regexString = dateJoinedPattern, idx = 3))
      .setInputCol("dateJoined")
      .setOutputCol("year")

    Seq(dayExtractor, monthExtractor, yearExtractor)
  }

  def main(args: Array[String]): Unit = {

    println("Sample data of employees:")
    val sampleEmployeeData = createSampleData
    sampleEmployeeData.show()

    println("Transformed sample data of employees")
    val transformations = buildNameTransformers ++ buildPhoneNumberTransformers ++ buildDateJoinedTransformers
    val pipeline = buildPipeline(sampleEmployeeData, transformations.map(_.asInstanceOf[Transformer]).toArray)
    val transformedData = transformData(sampleEmployeeData, pipeline)
    transformedData.show()

    println("Serialising pipeline to mleap bundle")
    val bundleFileName = "mleap_bundle.zip"
    val bundlePath = s"jar:file:${System.getProperty("user.dir")}/$bundleFileName"
    new SimpleSparkSerializer().serializeToBundle(
      transformer = pipeline,
      path = bundlePath,
      dataset = transformedData
    )
    println(s"Bundle serialized to $bundleFileName")
  }
}
