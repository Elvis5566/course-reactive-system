package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props, Terminated}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class SnapshotAckResult(s: ActorRef, id: Long, ack: SnapshotAck)
  case class SnapshotAckTimeOut(s: ActorRef, snapshot: Snapshot, replicate: Replicate)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  implicit val timeout: Timeout = 1.second

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L

  context.watch(replica)

  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = normal

  def normal: Receive = {
    case msg @ Replicate(key, valueOption, id) =>
      val s = sender()
      val seq = nextSeq()
//      log.info("{} get Replicate value {}, id {}, seq: {}", self, valueOption, id, seq)
      val g = (seq, (s, msg)) // don't know why need to assign to a variable first
      acks += g

      sendSnapshot(s, Snapshot(key, valueOption, seq), msg)

    case SnapshotAckResult(s, id, ack) =>
      acks -= ack.seq
      s ! Replicated(ack.key, id)

    case SnapshotAckTimeOut(s, snapshot, replicate) =>
      sendSnapshot(s, snapshot, replicate)

    case Terminated(_) =>
      log.info("Replicator replica Terminated")
      context.stop(self)
  }

  def sendSnapshot(s: ActorRef, snapshot: Snapshot, replicate: Replicate) = {
    val timer = context.system.scheduler.scheduleOnce(150.milliseconds, self, SnapshotAckTimeOut(s, snapshot, replicate))

    (replica ? snapshot).mapTo[SnapshotAck]
      .map{ack =>
        timer.cancel()
        SnapshotAckResult(s, replicate.id, ack)
      }.pipeTo(self)

  }

  override def postStop(): Unit = {
    acks.values.foreach { case (s, Replicate(key, valueOption, id)) =>
      s ! Replicated(key, id)
    }
  }
}
