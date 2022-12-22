import georyetti.regex.mleap.model.RegexExtractModel
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.mleap.transformer.RegexExtract
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

  def transformData(dataFrame: DataFrame, transformations: Seq[RegexExtract]): DataFrame = {
    val pipeline = new Pipeline().setStages(transformations.toArray.map(_.asInstanceOf[Transformer]))
    val fitPipeline = pipeline.fit(dataFrame)
    fitPipeline.transform(dataFrame)
  }

  def buildNameTransformers: Seq[RegexExtract] = {
    val namePattern = "(\\w+),\\s(Mr|Ms|Mrs|Dr).?\\s(\\w+)"
    val surnameExtractor = new RegexExtract(model = RegexExtractModel(regexString = namePattern, idx = 1))
      .setInputCol("name")
      .setOutputCol("surname")

    val titleExtractor = new RegexExtract(model = RegexExtractModel(regexString = namePattern, idx = 2))
      .setInputCol("name")
      .setOutputCol("title")

    val forenameExtractor = new RegexExtract(model = RegexExtractModel(regexString = namePattern, idx = 3))
      .setInputCol("name")
      .setOutputCol("forename")

    Seq(titleExtractor, forenameExtractor, surnameExtractor)
  }

  def buildPhoneNumberTransformers: Seq[RegexExtract] = {
    val phoneNumberPattern = ".?(\\d{3}).*(\\d{3}).*(\\d{4})"
    val phoneAreaCodeExtractor = new RegexExtract(model = RegexExtractModel(regexString = phoneNumberPattern, idx = 1))
      .setInputCol("phoneNumber")
      .setOutputCol("area_code")

    val phoneExchangeExtractor = new RegexExtract(model = RegexExtractModel(regexString = phoneNumberPattern, idx = 2))
      .setInputCol("phoneNumber")
      .setOutputCol("exchange")

    val phoneLineNumberExtractor = new RegexExtract(model = RegexExtractModel(regexString = phoneNumberPattern, idx = 3))
      .setInputCol("phoneNumber")
      .setOutputCol("line_number")

    Seq(phoneAreaCodeExtractor, phoneExchangeExtractor, phoneLineNumberExtractor)
  }

  def buildDateJoinedTransformers: Seq[RegexExtract] = {
    val dateJoinedPattern = "(\\d{2}).(\\d{2}).(\\d{4})"
    val dayExtractor = new RegexExtract(model = RegexExtractModel(regexString = dateJoinedPattern, idx = 1))
      .setInputCol("dateJoined")
      .setOutputCol("day")

    val monthExtractor = new RegexExtract(model = RegexExtractModel(regexString = dateJoinedPattern, idx = 2))
      .setInputCol("dateJoined")
      .setOutputCol("month")

    val yearExtractor = new RegexExtract(model = RegexExtractModel(regexString = dateJoinedPattern, idx = 3))
      .setInputCol("dateJoined")
      .setOutputCol("year")

    Seq(dayExtractor, monthExtractor, yearExtractor)
  }

  def main(args: Array[String]) = {

    println("Sample data of employees:")
    val sampleEmployeeData = createSampleData
    sampleEmployeeData.show()

    println("Transformed sample data of employees")
    val transformations = buildNameTransformers ++ buildPhoneNumberTransformers ++ buildDateJoinedTransformers
    val transformedData = transformData(sampleEmployeeData, transformations)
    transformedData.show()
  }
}
