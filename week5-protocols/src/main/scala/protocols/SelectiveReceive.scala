package protocols

import akka.actor.typed.{ActorContext, _}
import akka.actor.typed.scaladsl.{Behaviors, StashBuffer, StashOverflowException}
import akka.actor.typed.Behavior.{IgnoreBehavior, canonicalize, interpretMessage, start, validateAsInitial}


object SelectiveReceive {
    /**
      * @return A behavior that stashes incoming messages unless they are handled
      *         by the underlying `initialBehavior`
      * @param bufferSize Maximum number of messages to stash before throwing a `StashOverflowException`
      *                   Note that 0 is a valid size and means no buffering at all (ie all messages should
      *                   always be handled by the underlying behavior)
      * @param initialBehavior Behavior to decorate
      * @tparam T Type of messages
      *
      * Hint: Implement an [[ExtensibleBehavior]], use a [[StashBuffer]] and [[Behavior]] helpers such as `start`,
      * `validateAsInitial`, `interpretMessage`,`canonicalize` and `isUnhandled`.
      */
    def apply[T](bufferSize: Int, initialBehavior: Behavior[T]): Behavior[T] =
        new SelectiveReceive[T](bufferSize, initialBehavior)
}

class SelectiveReceive[T](bufferSize: Int, initialBehavior: Behavior[T]) extends ExtensibleBehavior[T] {
    def receive(ctx: ActorContext[T], msg: T): Behavior[T] = {
        println("SelectiveReceive receive: " + msg)
        val started = validateAsInitial(start(initialBehavior, ctx))
        stashIfUnhandled(started, msg, ctx, StashBuffer[T](bufferSize))
    }

    def stashIfUnhandled(b: Behavior[T], msg: T, ctx: ActorContext[T], buffer: StashBuffer[T]): Behavior[T] = {
        val next = interpretMessage(b, ctx, msg)
        val cNext = canonicalize(next, b, ctx)

        if (Behavior.isUnhandled(next)) {
            println("isUnhandled")
            if (buffer.isFull) {
                throw new StashOverflowException("buffer full")
            } else {
                buffer.stash(msg)
                waiting(b, buffer)
            }
        } else {
            println("handled")
            if (buffer.isEmpty) {
                println("buffer is empty")
                waiting(cNext, buffer)
            } else {
                println("buffer is not empty size: " + buffer.size)
                buffer.unstashAll(ctx.asScala, waiting(cNext, StashBuffer[T](bufferSize)))
            }
        }
    }

    def waiting(b: Behavior[T], buffer: StashBuffer[T]): Behavior[T] = Behaviors.setup[T] { ctx =>

        def behavior(b: Behavior[T]): Behavior[T] = Behaviors.receive[T] { case (ctx, msg) =>
            println("waiting get: " + msg)
            stashIfUnhandled(b, msg, ctx, buffer)
        }

        behavior(b)
    }

    override def receiveSignal(ctx: ActorContext[T], msg: Signal): Behavior[T] = Behaviors.same
}