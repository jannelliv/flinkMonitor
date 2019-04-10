package ch.ethz.infsec.benchmark

class BenchmarkRunner(val batchSize: Int, val reportInterval: Int, val warmupTime: Int) {
  require(1 <= batchSize)
  require(1 <= reportInterval)
  require(0 <= warmupTime)

  def printComment(comment: String): Unit = println("# " + comment)

  def measure[T](step: () => (T, Long)): Unit = {
    var running = true
    var phaseStart = 0L
    var phaseEnd = 0L
    var currentTime = 0L

    // warm-up
    phaseStart = System.nanoTime()
    phaseEnd = phaseStart + warmupTime * 1000000L
    do {
      for (_ <- 0 until batchSize) {
        val (result, _) = step()
        if (result == null)
          running = false
      }
      currentTime = System.nanoTime()
    } while (running && currentTime < phaseEnd)
    if (!running) {
      printComment("benchmark ended during warm-up phase")
      return
    }

    // measuring phase
    do {
      var iterations = 0L
      var auxSum = 0L
      running = true
      phaseStart = System.nanoTime()
      phaseEnd = phaseStart + reportInterval * 1000000L
      do {
        iterations += batchSize
        for (_ <- 0 until batchSize) {
          val (result, aux) = step()
          if (result == null)
            running = false
          auxSum += aux
        }
        currentTime = System.nanoTime()
      } while (running && currentTime < phaseEnd)
      if (running) {
        val elapsedTime = currentTime - phaseStart
        val elapsedSeconds = elapsedTime.toDouble * 1.0e-9
        val averageThroughput = iterations.toDouble / elapsedSeconds
        val averageAux = auxSum.toDouble / elapsedSeconds
        println(elapsedTime.toString + "," + iterations.toString + "," + auxSum.toString + "," +
          averageThroughput.toString + "," + averageAux.toString)
      }
    } while (running)
  }
}
