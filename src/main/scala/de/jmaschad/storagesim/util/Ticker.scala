package de.jmaschad.storagesim.util

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import scala.util.Random

object Ticker {
    var theTicker: Option[Ticker] = None
    def apply(delay: Double, tick: () => Boolean) = theTicker.map(_.apply(delay, tick))
}

class Ticker extends SimEntity("ticker") {
    private val Tick = 22100
    private var ticker = Map.empty[String, (Double, () => Boolean)]

    Ticker.theTicker = Some(this)

    override def startEntity() = {
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent) = event.getTag() match {
        case Tick =>
            val id = event.getData() match {
                case d: String => d
                case _ => throw new IllegalStateException
            }
            assert(ticker.contains(id))
            if (ticker(id)._2())
                send(getId, ticker(id)._1, Tick, id)
            else
                ticker -= id

        case _ =>
            throw new IllegalArgumentException
    }

    def apply(delay: Double, tick: () => Boolean) = {
        // add some random in case someone uses the same tick several times 
        val id = tick.## + "-" + Random.nextInt
        ticker += id -> (delay, tick)
        send(getId, delay, Tick, id)
    }
}