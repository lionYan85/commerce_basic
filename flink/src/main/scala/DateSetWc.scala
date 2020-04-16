import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object DateSetWc {

  def main(args: Array[String]): Unit = {


    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val inputPath = tool.get("input")
    val outputPath = tool.get("output")

    val env = ExecutionEnvironment.getExecutionEnvironment

//    "/Users/lionyan/Desktop/SparkDemo/git_demo/hello.txt"
    val txtDataSet: DataSet[String] = env.readTextFile(inputPath)

    val aggSet: AggregateDataSet[(String, Int)] = txtDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    aggSet.writeAsCsv(outputPath).setParallelism(1)

    env.execute()

//    aggSet.print()

  }

}
