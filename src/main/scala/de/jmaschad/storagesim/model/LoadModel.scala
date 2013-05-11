package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.model.user.User

object LoadModel {
    private var load = Map.empty[StorageObject, Double]

    def getLoad(obj: StorageObject) = load.getOrElse(obj, 0.0)

    def setUsers(users: Set[User]) = {
        computeLoad(users)
    }

    private def computeLoad(users: Set[User]) = {
        val objUsersMap = users.foldLeft(Map.empty[StorageObject, Set[User]]) {
            case (ouMap, user) =>
                ouMap ++ { user.objects map { obj => obj -> { ouMap.getOrElse(obj, Set.empty) + user } } }
        }

        val unnormalizedLoads = objUsersMap map {
            case (obj, users) =>
                val meanGetInterval = { users map { _.meanGetInterval } sum } / users.size
                obj -> (obj.size * users.size) / meanGetInterval
        }

        val meanLoad = unnormalizedLoads.values.sum / unnormalizedLoads.size

        load = unnormalizedLoads map {
            case (obj, load) => obj -> { load / meanLoad }
        }
    }
}