package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.TransferModel
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.model.user.User

private[microcloud] class UserRequestHandler(
    log: String => Unit,
    sendNow: (Int, Int, Object) => Unit,
    storageSystem: StorageSystem,
    transfers: TransferModel,
    processing: ProcessingModel) {

    def process(event: SimEvent) = {
        val request = Request.fromEvent(event)
        log("received %s".format(request))
        request.requestType match {
            case RequestType.Get =>
                storageSystem.loadTransaction(request.storageObject) match {
                    case Some(trans) =>
                        val storageObject = request.storageObject
                        val target = request.user.getId

                        // ACK the request
                        sendNow(target, User.RequestAck, request)

                        // Start the upload 
                        transfers.upload(request.transferId, storageObject.size, target, (size, onFinish) => {
                            processing.loadAndUpload(size, trans, onFinish)
                        }, success => if (success) {
                            trans.complete
                            log(request + " done")
                            sendNow(request.user.getId, User.RequestDone, request)
                        } else {
                            trans.abort
                            log(request + " failed")
                            sendNow(request.user.getId, User.RequestFailed, request)
                        })

                    case None =>
                        sendNow(request.user.getId, User.RequestRst, request)
                }

            case _ => throw new IllegalArgumentException
        }
    }
}
