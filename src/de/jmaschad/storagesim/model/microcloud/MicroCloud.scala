package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent

import de.jmaschad.storagesim.LoggingEntity
import de.jmaschad.storagesim.model.Disposer
import de.jmaschad.storagesim.model.DownloadJob
import de.jmaschad.storagesim.model.GetObject
import de.jmaschad.storagesim.model.PutObject
import de.jmaschad.storagesim.model.UploadJob
import de.jmaschad.storagesim.model.User
import de.jmaschad.storagesim.model.UserRequest
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.storage.StorageSystem

object MicroCloud {
  private val Base = 10200
  val ProcUpdate = Base + 1
  val Boot = ProcUpdate + 1
  val Shutdown = Boot + 1
  val Kill = Shutdown + 1
  val UserRequest = Kill + 1
  val Status = UserRequest + 1
}

class MicroCloud(name: String, resourceCharacteristics: MicroCloudResourceCharacteristics, disposer: Disposer) extends SimEntity(name) with LoggingEntity {
  val status = new MicroCloudStatus
  var lastChainUpdate: Option[SimEvent] = None

  private val storageSystem = new StorageSystem(resourceCharacteristics.storageDevices)
  private val processing = new ResourceProvisioning(storageSystem, resourceCharacteristics.bandwidth, this)

  private var state: MicroCloudState = new OfflineState

  def storeObject(storageObject: StorageObject) = {
    storageSystem.storeObject(storageObject)
  }

  def scheduleProcessingUpdate(delay: Double) = {
    lastChainUpdate match {
      case None =>
        lastChainUpdate = Some(schedule(getId(), delay, MicroCloud.ProcUpdate))

      case Some(lastUpdate) =>
        val lastUpdateProcessed = lastUpdate.eventTime() < CloudSim.clock();
        if (lastUpdateProcessed) {
          lastChainUpdate = Some(schedule(getId(), delay, MicroCloud.ProcUpdate))
        }

        val newUpdateBeforeLast = delay < lastUpdate.eventTime();
        if (newUpdateBeforeLast) {
          CloudSim.cancelEvent(lastUpdate);
          lastChainUpdate = Some(schedule(getId(), delay, MicroCloud.ProcUpdate))
        }
    }
  }

  override def startEntity: Unit = {
    log("started")
    send(getId(), 0.0, MicroCloud.Boot)
  }

  override def shutdownEntity: Unit = log("shutdown")

  override def processEvent(event: SimEvent): Unit = event.getTag match {
    case MicroCloud.ProcUpdate => processing.update
    case _ => state.process(event)
  }

  private def switchState(newState: MicroCloudState): Unit = {
    state = newState
  }

  trait MicroCloudState {
    def process(event: SimEvent): Unit

    protected def stateLog(message: String): Unit = log("[%s] %s".format(getClass().getSimpleName(), message))
  }

  class OfflineState extends MicroCloudState {
    def process(event: SimEvent): Unit = event.getTag match {
      case MicroCloud.Boot =>
        stateLog("received boot request")
        sendNow(getId, MicroCloud.Status)
        switchState(new OnlineState)

      case _ => stateLog("dropped event " + event)
    }
  }

  class OnlineState extends MicroCloudState {
    def process(event: SimEvent): Unit = event.getTag() match {
      case MicroCloud.UserRequest =>
        val request = event.getData() match {
          case r: UserRequest => r
          case _ => throw new IllegalStateException
        }
        processRequest(request)

      case MicroCloud.Status =>
        sendNow(disposer.getId(), Disposer.MicroCloudStatus, status)
        send(getId(), Disposer.StatusInterval, MicroCloud.Status)

      case MicroCloud.Shutdown =>
        stateLog("received shutdown request")
        sendNow(disposer.getId(), Disposer.MicroCloudShutdown)
        switchState(new OfflineState)

      case MicroCloud.Kill =>
        stateLog("received kill request")
        processing.clear
        switchState(new OfflineState)

      case _ => log("dropped event " + event)
    }

    def processRequest(request: UserRequest): Unit = {
      def failed = send(request.user.getId(), 0.0, User.RequestFailed, request)

      def load(obj: StorageObject) = {
        storageSystem.startLoad(obj)
        processing.add(new UploadJob(obj, _ => {
          storageSystem.finishLoad(obj)
        }))
      }

      def store(obj: StorageObject) = {
        storageSystem.startStore(obj)
        processing.add(new DownloadJob(obj,
          success =>
            if (success) storageSystem.finishStore(obj)
            else storageSystem.abortStore(obj)))
      }

      request match {
        case req: GetObject =>
          val obj = req.storageObject
          if (storageSystem.contains(obj)) load(obj) else failed

        case req: PutObject =>
          val obj = req.storageObject
          if (storageSystem.allocate(obj)) store(obj) else failed

        case _ => throw new IllegalArgumentException
      }
    }
  }
}
