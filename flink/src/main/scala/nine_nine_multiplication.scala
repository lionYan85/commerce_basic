object nine_nine_multiplication {

  def main(args: Array[String]): Unit = {

    var i = 1
    for (i <- 1 to 9) {
      var j = 1
      for (j <- 1 to i) {
        print(i.toString + "*" + j.toString + "=" + i * j + "\t")
      }
      print("\n")
    }
    i += 1
  }

}
