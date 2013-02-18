package de.jmaschad.storagesim.model.processing

import de.jmaschad.storagesim.Units

trait Workload {
    def process(timeSpan: Double): Workload
    def expectedDuration: Double
    def isDone: Boolean
}

private[processing] abstract class NetworkTransfer(size: Double, bandwidth: => Double) extends Workload {
    override def expectedDuration: Double = size / bandwidth
    override def isDone = size < 1 * Units.Byte

    protected def progress(timeSpan: Double) = timeSpan * bandwidth
}

object Upload {
    def apply(size: Double, bandwidth: => Double) = new Upload(size, bandwidth)
}

private[processing] class Upload(size: Double, bandwidth: => Double)
    extends NetworkTransfer(size, bandwidth) {

    override def process(timeSpan: Double) = {
        Upload(size - progress(timeSpan), bandwidth)
    }
}

object Download {
    def apply(size: Double, bandwidth: => Double) = new Download(size, bandwidth)
}

private[processing] class Download(size: Double, bandwidth: => Double)
    extends NetworkTransfer(size, bandwidth) {

    override def process(timeSpan: Double) = {
        Download(size - progress(timeSpan), bandwidth)
    }

}

object DiskIO {
    def apply(size: Double, throughput: => Double) = new DiskIO(size, throughput)
}

private[processing] class DiskIO(size: Double, throughput: => Double) extends Workload {
    override def process(timeSpan: Double) = DiskIO(size - progress(timeSpan), throughput)
    override def expectedDuration: Double = size / throughput
    override def isDone = size < 1 * Units.Byte

    private def progress(timeSpan: Double) = timeSpan * throughput
}