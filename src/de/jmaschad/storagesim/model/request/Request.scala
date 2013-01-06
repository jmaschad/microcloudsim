package de.jmaschad.storagesim.model.request

import de.jmaschad.storagesim.model.storage.StorageObject

private[request] object RequestType extends Enumeration {
  val Get = Value("GET")
  val Put = Value("PUT")
}
import RequestType._

trait Request {
  val requestType: RequestType.Value
  val storageObject: StorageObject
  val time: Double

  override def toString = "%s request for %s".format(requestType, storageObject)
}

class GetRequest(val storageObject: StorageObject, val time: Double) extends Request { val requestType = Get }
class PutRequest(val storageObject: StorageObject, val time: Double) extends Request { val requestType = Put }

