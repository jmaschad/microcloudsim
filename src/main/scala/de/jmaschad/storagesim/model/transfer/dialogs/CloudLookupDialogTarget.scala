package de.jmaschad.storagesim.model.transfer.dialogs

abstract sealed class CloudLookupDialogTarget
case class Lookup(request: RestDialog) extends CloudLookupDialogTarget
case class Result(cloud: Int) extends CloudLookupDialogTarget