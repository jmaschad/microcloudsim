package de.jmaschad.storagesim.model.user

import org.apache.commons.math3.distribution.RealDistribution
import org.apache.commons.math3.distribution.IntegerDistribution
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.transfer.dialogs.RestDialog

object RequestType extends Enumeration {
    type RequestType = Value
    val Get = Value("GET")
}
import RequestType._

object UserBehavior {
    def apply(delayModel: RealDistribution, requestType: RequestType) =
        new UserBehavior(delayModel, requestType)
}

class UserBehavior(val delayModel: RealDistribution, val requestType: RequestType)

