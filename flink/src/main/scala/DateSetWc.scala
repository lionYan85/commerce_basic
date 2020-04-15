import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import org.apache.flink.api.scala._

object DateSetWc {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val txtDataSet: DataSet[String] = env.readTextFile("/Users/lionyan/Desktop/SparkDemo/git_demo/hello.txt")

    val aggSet: AggregateDataSet[(String, Int)] = txtDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    aggSet.print()

  }

}
