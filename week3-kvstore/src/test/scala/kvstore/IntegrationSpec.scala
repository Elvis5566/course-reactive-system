/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.Props
import akka.testkit.TestProbe
import kvstore.Replicator.{Snapshot, SnapshotAck}
import org.scalatest.{FunSuiteLike, Matchers}

trait IntegrationSpec
  extends FunSuiteLike
        with Matchers { this: KVStoreSuite =>

  import Arbiter._

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */


  test("Integration-case1: unrealiable primary persistence") {
    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), "Integration-case1-primary")
    val user = session(primary)
//    val secondary = TestProbe()

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)
    arbiter.send(primary, Replicas(Set(primary)))

    (0 to 50).foreach { i =>
      user.setAcked(s"k$i", s"v$i")
    }

  }
}
