package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout
import kvstore.Persistence.{Persist, Persisted}
import kvstore.Replica.ReplicatedResult
import kvstore.Replicator.Replicated

import scala.concurrent.{Future, TimeoutException}

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class ReplicatedResult(s: ActorRef, id: Long, replicatedList: List[Replicated])
  case class PersistedResult(s: ActorRef, peristed: Persisted, resendTimer: Cancellable)
  case class PersistedTimeout(s: ActorRef, perist: Persist)
  case class MainPersistedResult(s: ActorRef, id: Long)
  case class MainPersistedTimeout(s: ActorRef, perist: Persist, replicatedResultFuture: Future[ReplicatedResult], operationFailTimer: Cancellable)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  implicit val timeout: Timeout = 1.second

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persistence: ActorRef = startPersistence()

  arbiter ! Join


  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ => SupervisorStrategy.Resume
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      log.info("Insert key: {}, value: {}, id: {}", key, value, id)
      val s = sender()

      kv += key -> value

      val replicatedResultFuture = Future.sequence(replicators.toList.map { replicator =>
        (replicator ? Replicate(key, Some(value), id)).mapTo[Replicated]
      }).map(r => ReplicatedResult(s, id, r))

      val operationFailTimer = context.system.scheduler.scheduleOnce(1.second, s, OperationFailed(id))
      sendMainPersis(s, Persist(key, Some(value), id), replicatedResultFuture, operationFailTimer)

    case Remove(key, id) =>
      val s = sender()
      kv -= key

      val replicatedResultFuture = Future.sequence(replicators.toList.map { replicator =>
        (replicator ? Replicate(key, None, id)).mapTo[Replicated]
      }).map(r => ReplicatedResult(s, id, r))

      val operationFailTimer = context.system.scheduler.scheduleOnce(1.second, s, OperationFailed(id))
      sendMainPersis(s, Persist(key, None, id), replicatedResultFuture, operationFailTimer)
//      sender ! OperationAck(id)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) =>
      log.info("Get Replicas: " + replicas)
      (secondaries.keys.toSet -- replicas).foreach { replica =>
        log.info("Stop {}", replica)
        val replicator = secondaries(replica)
        replicators -= replicator
        context.stop(replicator)
        secondaries -= replica

      }

      replicas.filterNot(_ == self).foreach { replica =>

        if (secondaries.get(replica).isEmpty) {
          val replicator = context.watch(context.actorOf(Replicator.props(replica)))
          secondaries += replica -> replicator
          replicators += replicator

          kv.foreach { case (k, v) =>
              replicator ! Replicate(k, Some(v), 0)
          }

        }
      }

    case MainPersistedResult(s, id) =>
      s ! OperationAck(id)

    case MainPersistedTimeout(s, persist, replicatedResultFuture, persistenceTimer) =>
      sendMainPersis(s, persist, replicatedResultFuture, persistenceTimer)

    case Terminated(e) =>
      log.info("{} Terminated {}", sender(), e)
      replicators -= sender()
//      startPersistence()

//    case m =>
//      log.info("leader get {}", m)
  }

  def startPersistence() = context.actorOf(persistenceProps)

  def sendMainPersis(s: ActorRef, persist: Persist, replicatedResultFuture: Future[ReplicatedResult], operationFailTimer: Cancellable) = {
    log.info("sendMainPersis id: {}", persist.id)
    val resendTimer = context.system.scheduler.scheduleOnce(100.milliseconds, self, MainPersistedTimeout(s, persist, replicatedResultFuture, operationFailTimer))

//    implicit val timeout: Timeout = 100.milliseconds
    val mainPersistFuture = (persistence ? persist).mapTo[Persisted]

    mainPersistFuture.flatMap { persisted =>
      resendTimer.cancel()
      replicatedResultFuture
    }.map {_ =>
      log.info("cancel persistenceFailTimer id: {}", persist.id)
      operationFailTimer.cancel()
      MainPersistedResult(s, persist.id) // add additional class and send to self to avoid sending Failure to client, which will cause Step5-case4 fail
    }.pipeTo(self)

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = expect(0L)

  def expect(expectedSeq: Long): Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) =>
//      log.info("{} get Snapshot", self)
      val s = sender()

      if (seq < expectedSeq) {
        s ! SnapshotAck(key, seq)
      } else if (seq == expectedSeq){

        if (valueOption.nonEmpty) {
          kv += key -> valueOption.get
        } else {
          kv -= key
        }

        sendPersist(s, Persist(key, valueOption, seq))
      }

    case PersistedResult(s, Persisted(key, seq), resendTimer) =>
//      log.info("{} get Snapshot", self)
      resendTimer.cancel()
      s ! SnapshotAck(key, seq)
      context become expect(seq + 1)

    case PersistedTimeout(s, persist) =>
      sendPersist(s, persist)

//    case Terminated(e) =>
//      log.info("{} Terminated {}", sender(), e)
//      startPersistence()
  }

  def sendPersist(s: ActorRef, persist: Persist) = {
    val timer = context.system.scheduler.scheduleOnce(200.milliseconds, self, PersistedTimeout(s, persist))
    (persistence ? persist).mapTo[Persisted]
      .map(p => PersistedResult(s, p, timer))
      .pipeTo(self)
  }

}

