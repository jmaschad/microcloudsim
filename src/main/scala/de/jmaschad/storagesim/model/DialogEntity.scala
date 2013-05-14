package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import scala.util.Random
import org.cloudbus.cloudsim.core.CloudSim
import de.jmaschad.storagesim.model.distributor.Distributor
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.CloudSim

object DialogEntity {
    val Timeout = 10.0
    type TimeoutHandler = () => Unit
    type MessageHandler = AnyRef => Unit

    private val Base = 30000
    val DialogMessage = Base + 1
    val DialogTimeout = DialogMessage + 1
}

trait DialogEntity extends Entity {
    protected def dialogsEnabled: Boolean = true

    private var dialogs = Map.empty[String, Dialog]

    abstract override def processEvent(event: SimEvent): Unit = event.getTag match {
        case DialogEntity.DialogMessage if dialogsEnabled =>
            val message = event.getData() match {
                case m: Message => m
                case _ => throw new IllegalStateException
            }
            handleMessage(event.getSource(), message)

        case DialogEntity.DialogTimeout =>
            val timeout = event.getData match {
                case t: Timeout => t
                case _ => throw new IllegalStateException
            }
            handleTimeout(timeout)

        case _ =>
            super.processEvent(event)
    }

    abstract override protected def reset() = {
        dialogs = Map.empty[String, Dialog]
        super.reset()
    }

    def openDialog(target: Int): Dialog = {
        val targetEnity = Entity.entityForId(target)
        val avgDelay = NetworkDelay.between(netID, targetEnity.netID)

        if (avgDelay == 0.0 && !this.isInstanceOf[Distributor] && !targetEnity.isInstanceOf[Distributor]) {
            println("PROBLEM")
            println()
        }

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

    def sayWithTimeout(message: AnyRef, timeoutHandler: DialogEntity.TimeoutHandler, dialog: Dialog): Unit = {
        say(message, dialog)
        send(getId(), DialogEntity.Timeout, DialogEntity.DialogTimeout,
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
            createMessageHandler(dialog, message.content) match {
                case Some(handler) =>
                    dialog.messageHandler = handler
                    handleMessage(source, message)

                case None =>
                    throw new IllegalStateException
            }

        case _ =>
            // probably timeouts
            log("Received unknown message: " + message.content + " from " + CloudSim.getEntityName(source))
            throw new IllegalStateException
    }

    def handleTimeout(timeout: Timeout) =
        dialogs.get(timeout.dialog) foreach { dialog =>
            if (timeout.messageId == dialog.messageId) {
                timeout.handler()
                dialogs -= timeout.dialog
            }
        }

    protected def createMessageHandler(dialog: Dialog, content: AnyRef): Option[DialogEntity.MessageHandler]

    private def answerDialog(source: Int, message: Message): Dialog = {
        val avgDelay = NetworkDelay.between(netID, Entity.entityForId(source).netID)
        val dialog = new Dialog(source, this, avgDelay, message.dialog)

        assert(!dialogs.isDefinedAt(message.dialog))
        dialogs += dialog.id -> dialog

        dialog
    }
}
object Dialog {
    var counter = 0L
}

class Dialog(
    val partner: Int,
    val dialogEntity: DialogEntity,
    val averageDelay: Double,
    val id: String = Dialog.counter.toString()) {
    Dialog.counter += 1

    var messageHandler: DialogEntity.MessageHandler = (_) => throw new IllegalStateException
    var messageId = 0L

    def say(message: AnyRef, timeoutHandler: DialogEntity.TimeoutHandler) = {
        dialogEntity.sayWithTimeout(message, timeoutHandler, this)
    }

    def close() = dialogEntity.closeDialog(this)

    def sayAndClose(message: AnyRef) = {
        dialogEntity.say(message, this)
        close()
    }
}

class Message(val dialog: String, val content: AnyRef, val init: Boolean)
class Timeout(val dialog: String, val messageId: Long, val handler: () => Unit)