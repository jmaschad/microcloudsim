package de.jmaschad.storagesim.model.transfer

import scala.util.Random
import org.cloudbus.cloudsim.core.SimEvent
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.ProcessingEntity
import DialogCenter._
import de.jmaschad.storagesim.model.DialogEntity
import de.jmaschad.storagesim.model.Entity
import de.jmaschad.storagesim.model.NetworkDelay

object DialogCenter {
    val Timeout = 3.0
    type TimeoutHandler = () => Unit
    type MessageHandler = AnyRef => Unit
}

class DialogCenter(
    entity: DialogEntity,
    handlerFactory: (Dialog, AnyRef) => Option[MessageHandler],
    send: (Int, Double, Int, Object) => SimEvent) {

    private var dialogs = Map.empty[String, Dialog]

    def openDialog(target: Int): Dialog = {
        val avgDelay = NetworkDelay.between(entity.region, Entity.entityForId(target).region)
        val dialog = new Dialog(target, this, avgDelay)
        assert(!dialogs.isDefinedAt(dialog.id))
        dialogs += dialog.id -> dialog
        dialog
    }

    def closeDialog(dialog: Dialog) = {
        assert(dialogs.isDefinedAt(dialog.id))
        dialogs -= dialog.id
    }

    def say(message: AnyRef, dialog: Dialog): Unit = {
        val init = dialog.messageId == 0
        // add the message delay once to every dialog
        val delay = if (init) dialog.averageDelay else 0.0
        send(dialog.partner, delay, DialogEntity.DialogMessage, new Message(dialog.id, message, init))
    }

    def sayWithTimeout(message: AnyRef, timeoutHandler: TimeoutHandler, dialog: Dialog): Unit = {
        say(message, dialog)
        send(entity.getId(), DialogCenter.Timeout, DialogEntity.DialogTimeout,
            new Timeout(dialog.id, dialog.messageId, timeoutHandler))
    }

    def handleMessage(source: Int, message: Message): Unit = dialogs.get(message.dialog) match {
        // message for existing dialog
        case Some(dialog) =>
            dialog.messageId += 1
            dialog.messageHandler(message.content)

        // first message of new dialog
        case None if message.init =>
            val dialog = answerDialog(source, message)
            handlerFactory(dialog, message.content) match {
                case Some(handler) =>
                    dialog.messageHandler = handler
                    handleMessage(source, message)

                case None =>
                    throw new IllegalStateException
            }

        case _ =>
            // probably timeouts
            throw new IllegalStateException
    }

    def handleTimeout(timeout: Timeout) =
        dialogs.get(timeout.dialog) foreach { dialog =>
            if (timeout.messageId == dialog.messageId) {
                timeout.handler()
                dialogs -= timeout.dialog
            }
        }

    private def answerDialog(source: Int, message: Message): Dialog = {
        val avgDelay = NetworkDelay.between(entity.region, Entity.entityForId(source).region)
        val dialog = new Dialog(source, this, avgDelay, message.dialog)

        assert(!dialogs.isDefinedAt(message.dialog))
        dialogs += dialog.id -> dialog

        dialog
    }
}

class Dialog(
    val partner: Int,
    val dialogCenter: DialogCenter,
    val averageDelay: Double,
    val id: String = hashCode() + "-" + Random.nextInt) {

    var messageHandler: MessageHandler = (_) => throw new IllegalStateException
    var messageId = 0L

    def say(message: AnyRef, timeoutHandler: TimeoutHandler) = {
        dialogCenter.sayWithTimeout(message, timeoutHandler, this)
    }

    def close() = dialogCenter.closeDialog(this)

    def sayAndClose(message: AnyRef) = {
        dialogCenter.say(message, this)
        close()
    }
}

class Message(val dialog: String, val content: AnyRef, val init: Boolean)
class Timeout(val dialog: String, val messageId: Long, val handler: () => Unit)