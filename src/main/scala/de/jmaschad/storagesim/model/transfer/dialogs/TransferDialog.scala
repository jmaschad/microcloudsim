package de.jmaschad.storagesim.model.transfer.dialogs

abstract sealed class TransferDialog
case class FinishDownload extends TransferDialog
case class Packet(size: Double, timeSend: Double) extends TransferDialog
case class DownloadReady extends TransferDialog
case class Ack extends TransferDialog