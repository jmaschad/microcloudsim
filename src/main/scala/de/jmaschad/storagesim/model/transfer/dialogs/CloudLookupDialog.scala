package de.jmaschad.storagesim.model.transfer.dialogs

abstract sealed class CloudLookupDialog
case class Lookup(request: RestDialog) extends CloudLookupDialog
case class Result(cloud: Int) extends CloudLookupDialog