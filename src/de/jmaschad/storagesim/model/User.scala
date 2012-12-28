package de.jmaschad.storagesim.model

import scala.collection.mutable
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import org.apache.commons.math3.distribution.UniformRealDistribution
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.LoggingEntity

object User {
  private val Base = 10300
  val RequestFailed = Base + 1
}

class User(name: String, disposer: Disposer) extends SimEntity(name) with LoggingEntity {
  private val behaviors = mutable.ArrayBuffer.empty[Behavior]

  def addBehavior(behavior: Behavior) = behaviors += behavior

  override def startEntity(): Unit = {
    log("started")

    behaviors.foreach(b =>
      b.requests.foreach(req => send(disposer.getId(), req._1, Disposer.UserRequest, req._2)))
  }

  override def shutdownEntity(): Unit = log("shutdown")
  override def processEvent(event: SimEvent): Unit = log("received event " + event)
}