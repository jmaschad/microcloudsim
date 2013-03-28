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

object NetUp {
    def apply(size: Double, bandwidth: => Double) = new NetUp(size, bandwidth)
}

private[processing] class NetUp(size: Double, bandwidth: => Double)
    extends NetworkTransfer(size, bandwidth) {

    override def process(timeSpan: Double) = {
        NetUp(size - progress(timeSpan), bandwidth)
    }
}

object NetDown {
    def apply(size: Double, bandwidth: => Double) = new NetDown(size, bandwidth)
}

private[processing] class NetDown(size: Double, bandwidth: => Double)
    extends NetworkTransfer(size, bandwidth) {

    override def process(timeSpan: Double) = {
        NetDown(size - progress(timeSpan), bandwidth)
    }

}