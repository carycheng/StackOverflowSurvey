import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count

object StackOverflowSurvey {

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)

		val sparkSession = SparkSession.builder()
				.appName("StackOverflowSurveyResponse")
				.config("spark.master", "local[*]")
				.getOrCreate()
		import sparkSession.implicits._

		val dataFrameReader = sparkSession.read

		val responses = dataFrameReader
				.format("csv")
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/data/survey_results_public.csv")

		responses.groupBy("Country").count().sort($"count".desc).show(100,false)
	}
}
