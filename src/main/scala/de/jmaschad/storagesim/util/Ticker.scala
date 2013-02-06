package de.jmaschad.storagesim.util

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import scala.util.Random

object Ticker {
    val Tick = 22100

    def apply(delay: Double, tick: => Boolean) = new Ticker(delay, tick)
}

private[util] class Ticker(delay: Double, tick: => Boolean) extends SimEntity("ticker" + Random.nextInt) {
    override def startEntity() = {
        send(getId, delay, Ticker.Tick)
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent) = event.getTag() match {
        case Ticker.Tick =>
            if (tick)
                send(getId, delay, Ticker.Tick)

        case _ =>
            throw new IllegalArgumentException
    }
}