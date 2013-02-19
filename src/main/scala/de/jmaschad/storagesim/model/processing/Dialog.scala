package de.jmaschad.storagesim.model.processing

import scala.util.Random
import de.jmaschad.storagesim.model.ProcessingEntity

class Message(val dialogId: String, val messageId: Long, val content: AnyRef)
class Timeout(val dialogId: String, val isTimeout: Long => Boolean)

class Dialog(
    send: (Int, Double, Int, Object) => _,
    selfId: Int,
    partner: Int,
    procMessage: AnyRef => Unit,
    val timeout: Double,
    val onTimeout: () => Unit,
    val dialogId: String = hashCode() + "-" + Random.nextInt) {

    private var canSay = true

    private var _messageId: Long = 0
    def messageId = _messageId

    def say(message: AnyRef) =
        if (canSay) {
            send(partner, 0.0, ProcessingEntity.DialogMessage, new Message(dialogId, messageId, message))

            if (timeout > 0) {
                val capturedId = messageId
                send(selfId, timeout, ProcessingEntity.DialogTimeout, new Timeout(dialogId, (id: Long) => { id == capturedId }))
            }

            canSay = false
        } else {
            throw new IllegalStateException
        }

    def processMessage(message: AnyRef) = {
        _messageId += 1
        canSay = true

        procMessage(message)
    }
}
