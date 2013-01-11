package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.Disposer
import de.jmaschad.storagesim.model.DownloadJob
import de.jmaschad.storagesim.model.UploadJob
import de.jmaschad.storagesim.model.request.Request
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.storage.StorageSystem
import de.jmaschad.storagesim.model.User
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.request.RequestType

object MicroCloud {
    private val Base = 10200
    val ProcUpdate = Base + 1
    val Boot = ProcUpdate + 1
    val Shutdown = Boot + 1
    val Kill = Shutdown + 1
    val UserRequest = Kill + 1
    val Status = UserRequest + 1
}

class MicroCloud(name: String, resourceCharacteristics: MicroCloudResourceCharacteristics, initialObjects: Iterable[StorageObject], disposer: Disposer) extends SimEntity(name) {
    private val log = Log.line("MicroCloud '%s'".format(getName), _: String)
    private val storageSystem = new StorageSystem(resourceCharacteristics.storageDevices, initialObjects)
    private val processing = new ResourceProvisioning(storageSystem, resourceCharacteristics.bandwidth, this)
    private var state: MicroCloudState = new OfflineState

    var lastChainUpdate: Option[SimEvent] = None

    def scheduleProcessingUpdate(delay: Double) = {
        lastChainUpdate.map(CloudSim.cancelEvent(_))
        lastChainUpdate = Some(schedule(getId(), delay, MicroCloud.ProcUpdate))
    }

    def status = Status(storageSystem.buckets)

    override def startEntity(): Unit = {
        send(getId(), 0.0, MicroCloud.Boot)
    }

    override def shutdownEntity() = {}

    override def processEvent(event: SimEvent): Unit = event.getTag match {
        case MicroCloud.ProcUpdate =>
            log("chain update")
            processing.update()
        case _ => state.process(event)
    }

    override def toString = "%s %s".format(getClass.getSimpleName, getName)

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
                    case r: Request => r
                    case _ => throw new IllegalStateException
                }
                stateLog("received %s".format(request))
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

        def processRequest(request: Request): Unit = {
            def failed() = sendNow(request.user.getId, User.RequestFailed, request)
            def done() = sendNow(request.user.getId, User.RequestDone, request)

            def load(obj: StorageObject) = {
                storageSystem.startLoad(obj)
                processing.add(new UploadJob(obj, _ => {
                    storageSystem.finishLoad(obj)
                    done()
                }))
            }

            def store(obj: StorageObject) = {
                storageSystem.startStore(obj)
                processing.add(new DownloadJob(obj,
                    success =>
                        if (success) {
                            storageSystem.finishStore(obj)
                            done()
                        } else {
                            storageSystem.abortStore(obj)
                            failed()
                        }))
            }

            request.requestType match {
                case RequestType.Get =>
                    val obj = request.storageObject
                    if (storageSystem.contains(obj)) load(obj) else failed

                case RequestType.Put =>
                    val obj = request.storageObject
                    if (storageSystem.allocate(obj)) store(obj) else failed

                case _ => throw new IllegalArgumentException
            }
        }
    }
}
