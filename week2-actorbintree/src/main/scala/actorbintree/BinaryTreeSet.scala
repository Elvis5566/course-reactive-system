/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout

import scala.collection.immutable.Queue
import scala.concurrent.Future
import scala.concurrent.duration._

object BinaryTreeSet {

  trait GeneralOperation {
    def requester: ActorRef
    def id: Int
  }

  trait Operation extends GeneralOperation {
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

  case class InOrder(requester: ActorRef, id: Int) extends GeneralOperation
  case class InOrdered(id: Int, ordered: List[Int]) extends OperationReply

  case class GetHeight(requester: ActorRef, id: Int) extends GeneralOperation
  case class Height(id: Int, height: Int) extends OperationReply
}

class TestActor extends Actor with ActorLogging {
  
  def receive = contextA

  def contextA: Receive = {
    case "GG" =>
      context become contextB
      context become contextA
      self ! "Hi"
    case "Hi" => log.info("contextA Hi")
  }

  def contextB: Receive = {
    case "Hi" => log.info("contextB Hi")
  }
}

class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._
  import context.dispatcher

  implicit val timeout = Timeout(10 seconds)

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[GeneralOperation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case msg @ GC =>
      log.info(msg.toString)
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context become garbageCollecting(newRoot)

    case msg @ InOrder(requester, id) =>
      log.info(msg.toString)
      (root ? msg).mapTo[InOrdered] pipeTo requester

    case msg @ GetHeight(requester, id) =>
      (root ? msg).mapTo[Height] pipeTo requester

    case msg: GeneralOperation => root ! msg
      log.info(msg.toString)

  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
//    case GC =>
    case CopyFinished =>
      log.info("CopyFinished")

      root = newRoot

      pendingQueue.foreach { op =>
        log.info("dequeue " + op)
        normal(op)
      }

      pendingQueue = Queue.empty

      context become normal

      // Challenge solution
//      (newRoot ? InOrder(self, 0)).mapTo[InOrdered].map { case InOrdered(_, sortedList) =>
//        val inOrderRoot = createRoot
//        val inOrder = inOrderTraversal(sortedList)
//        log.info("inOrderTraversal: " + inOrder)
//
//        inOrder.foreach { i =>
//          inOrderRoot ! Insert(self, 0, i)
//        }
//
//        newRoot ! PoisonPill
//
//        root = inOrderRoot
//
//        pendingQueue.foreach { op =>
//          log.info("dequeue " + op)
//          normal(op)
//        }
//
//        pendingQueue = Queue.empty
//
//        context become normal
//      }

    case op: GeneralOperation =>
//      log.info("enqueue " + op)
      pendingQueue = pendingQueue.enqueue(op)

  }

  def inOrderTraversal(sortedList: List[Int]): List[Int] =  {
    if (sortedList.size < 3) {
      sortedList
    } else {
      val length = sortedList.size
      val mid = length / 2
      val (left, right) = sortedList.splitAt(mid)

      right.head :: inOrderTraversal(left) ::: inOrderTraversal(right.tail) // TODO: it's a better to handle it in parallel.
    }
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._
  import context.dispatcher

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved
  implicit val timeout = Timeout(10 seconds)

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case msg @ Insert(requester, id, e) =>
      if (elem == e) {
        removed = false
        requester ! OperationFinished(id)
      } else if (e < elem) {
        insert(Left, msg)
      } else {
        insert(Right, msg)
      }

    case msg @ Contains(requester, id, e) =>
      if (elem == e) {
        requester ! ContainsResult(id, !removed)
      } else if (e < elem) {
        contains(Left, msg)
      } else {
        contains(Right, msg)
      }
    case msg @ Remove(requester, id, e) =>
      if (elem == e) {
        removed = true
        requester ! OperationFinished(id)
      } else if (e < elem) {
        remove(Left, msg)
      } else {
        remove(Right, msg)
      }

    case msg @ CopyTo(treeNode) =>
      val subActors = subtrees.values.toSet

      if (!removed) {
        treeNode ! Insert(self, 0, elem)
      }

      subActors.foreach(_ ! msg)
      self ! CopyFinished
      context become copying(subActors, removed)

    case msg @ InOrder(_, id) =>
//      log.info(s"elem: $elem get inOrder")
      val s = sender()
      val selfList = if (removed) Nil else List(elem)

      val inOrderFuture = { position: Position =>
        subtrees.get(position).map(a => (a ? msg).mapTo[InOrdered]).getOrElse(Future.successful(InOrdered(id, Nil)))
      }
      val leftInOrderFuture = inOrderFuture(Left)
      val rightInOrderFuture = inOrderFuture(Right)

      val resultFuture = for {
        InOrdered(_, left) <- leftInOrderFuture
        InOrdered(_, right) <- rightInOrderFuture
      } yield {
        val inOrdered = left :::  selfList ::: right
//        log.info(s"elem: $elem inOrdered: $inOrdered")
        InOrdered(id, inOrdered)
      }

      resultFuture pipeTo s

    case msg @ GetHeight(_, id) =>
      val s = sender()

      val heightFuture = { position: Position =>
        subtrees.get(position).map(a => (a ? msg).mapTo[Height]).getOrElse(Future.successful(Height(id, 0)))
      }

      val leftHeightFuture = heightFuture(Left)
      val rightHeightFuture = heightFuture(Right)

      val resultFuture = for {
        Height(_, leftHeight) <- leftHeightFuture
        Height(_, rightHeight) <- rightHeightFuture
      } yield {
        val height = (leftHeight max rightHeight) + 1
        Height(id, height)
      }

      resultFuture pipeTo s
  }

  private def insert(position: Position, msg: Insert): Unit = {
    val subTreeOpt = subtrees.get(position)
    if (subTreeOpt.isDefined) {
      subTreeOpt.get ! msg
    } else {
      val Insert(requester, id, e) = msg
      subtrees += position -> context.actorOf(BinaryTreeNode.props(e, false))
      requester ! OperationFinished(id)
    }
  }

  private def contains(position: Position, msg: Contains): Unit = {
    val subTreeOpt = subtrees.get(position)
    if (subTreeOpt.isDefined) {
      subTreeOpt.get ! msg
    } else {
      val Contains(requester, id, _) = msg
      requester ! ContainsResult(id, false)
    }
  }

  private def remove(position: Position, msg: Remove): Unit = {
    val subTreeOpt = subtrees.get(position)
    if (subTreeOpt.isDefined) {
      subTreeOpt.get ! msg
    } else {
      val Remove(requester, id, _) = msg
      requester ! OperationFinished(id)
    }
  }
  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case msg @ OperationFinished(id) =>
      self ! CopyFinished

      context become copying(expected, true)
    case msg @ CopyFinished =>
      val newExpected = expected - sender

      if (newExpected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.stop(self)
//        self ! PoisonPill
      }

      context become copying(newExpected, insertConfirmed)
  }

}
