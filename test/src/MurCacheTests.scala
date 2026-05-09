package digital.junkie.murcache

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.parallel.*
import scala.concurrent.Future
import scala.concurrent.duration.*
import utest.*

object MurCacheTests extends TestSuite:
  private def run[A](ioa: IO[A]): Future[A] =
    ioa.unsafeToFuture()

  def tests = Tests {

    test("get returns fetched value") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO.pure(key.length),
          size = 10
        )
        result <- cache.get("hello")
      yield result

      run(test.map(result => assert(result == 5)))
    }

    test("zero-sized cache behaves as a noop cache") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 0
        )
        _ <- cache.put("hello", 99)
        first <- cache.get("hello")
        second <- cache.get("hello")
        invalidated <- cache.invalidate("hello")
        _ <- cache.invalidateAll
      yield (first, second, invalidated)

      run(test.map { case (first, second, invalidated) =>
        assert(first == 5)
        assert(second == 5)
        assert(invalidated == false)
        assert(callCount == 2)
      })
    }

    test("noop cache never stores values") {
      var callCount = 0
      val cache =
        MurCache.noop[IO, String, Int](key => IO { callCount += 1; key.length })

      val test = for
        _ <- cache.put("hello", 99)
        first <- cache.get("hello")
        second <- cache.get("hello")
        invalidated <- cache.invalidate("hello")
        _ <- cache.invalidateAll
      yield (first, second, invalidated)

      run(test.map { case (first, second, invalidated) =>
        assert(first == 5)
        assert(second == 5)
        assert(invalidated == false)
        assert(callCount == 2)
      })
    }

    test("get caches: fetch called only once per key") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        first <- cache.get("hello")
        second <- cache.get("hello")
      yield (first, second)

      run(test.map { case (first, second) =>
        assert(first == 5)
        assert(second == 5)
        assert(callCount == 1)
      })
    }

    test("fetch failure is propagated to waiting fibers") {
      val error = new RuntimeException("fetch failed") {
        override def fillInStackTrace() = this
      }

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.sleep(50.millis) >> IO.raiseError(error),
          size = 10
        )
        fiber1 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        fiber2 <- cache.get("hello").start
        _ <- fiber1.join
        result <- fiber2.joinWithNever.timeout(500.millis).attempt
      yield result

      run(test.map(result => assert(result == Left(error))))
    }

    test("cancellation cleans up so subsequent gets do not hang") {
      val test = for
        attempts <- IO.ref(0)
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ =>
            attempts.getAndUpdate(_ + 1).flatMap { n =>
              if n == 0 then IO.sleep(3.seconds).as(0) else IO.pure(42)
            },
          size = 10
        )
        fiber <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- fiber.cancel.attempt
        result <- cache.get("hello").timeout(500.millis).attempt
      yield result

      run(test.map(result => assert(result == Right(42))))
    }

    test("failed fetch is not cached: subsequent gets retry") {
      val error = new RuntimeException("boom") {
        override def fillInStackTrace() = this
      }
      var calls = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO { calls += 1 } >> IO.raiseError(error),
          size = 10
        )
        first <- cache.get("hello").attempt
        second <- cache.get("hello").attempt
      yield (first, second)

      run(test.map { case (first, second) =>
        assert(first == Left(error))
        assert(second == Left(error))
        assert(calls == 2)
      })
    }

    test("cancellation of one requestor does not cancel fetch for other waiters") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.sleep(100.millis).as(42),
          size = 10
        )
        fiber1 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        fiber2 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- fiber1.cancel
        result <- fiber2.joinWithNever.timeout(500.millis)
      yield result

      run(test.map(result => assert(result == 42)))
    }

    test("fetch is cancelled once the last waiter is cancelled") {
      val test = for
        cancelled <- IO.ref(false)
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.never[Int].onCancel(cancelled.set(true)),
          size = 10
        )
        fiber1 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        fiber2 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- fiber1.cancel
        didCancelEarly <- cancelled.get
        _ <- fiber2.cancel
        _ <- IO.sleep(50.millis)
        didCancelFinally <- cancelled.get
      yield (didCancelEarly, didCancelFinally)

      run(test.map { case (didCancelEarly, didCancelFinally) =>
        assert(didCancelEarly == false)
        assert(didCancelFinally == true)
      })
    }

    test("concurrent gets on same key wait on same Deferred") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch =
            key => IO.sleep(100.millis) >> IO { callCount += 1; key.length },
          size = 10
        )
        results <- List.fill(5)(cache.get("hello")).parSequence
      yield results

      run(test.map { results =>
        assert(callCount == 1)
        assert(results == List.fill(5)(5))
      })
    }

    test("delete returns true for existing key") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO.pure(key.length),
          size = 10
        )
        _ <- cache.get("hello")
        invalidated <- cache.invalidate("hello")
      yield invalidated

      run(test.map(invalidated => assert(invalidated == true)))
    }

    test("invalidate returns false for missing key") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO.pure(key.length),
          size = 10
        )
        invalidated <- cache.invalidate("hello")
      yield invalidated

      run(test.map(invalidated => assert(invalidated == false)))
    }

    test("invalidate evicts: subsequent get re-fetches") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        _ <- cache.get("hello")
        _ <- cache.invalidate("hello")
        _ <- cache.get("hello")
      yield ()

      run(test.map { _ =>
        assert(callCount == 2)
      })
    }

    test("invalidate of an in-flight key lets the original getter and waiters receive the fetched value") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.sleep(100.millis).as(42),
          size = 10
        )
        fiber1 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        fiber2 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.invalidate("hello")
        waiter <- fiber2.joinWithNever.timeout(500.millis)
        original <- fiber1.joinWithNever.timeout(500.millis)
      yield (original, waiter)

      run(test.map { case (original, waiter) =>
        assert(original == 42)
        assert(waiter == 42)
      })
    }

    test("invalidate of an in-flight key does not cancel its fetch") {
      val test = for
        cancelled <- IO.ref(false)
        cache <- MurCache.simple[IO, String, Int](
          fetch =
            _ => IO.sleep(100.millis).as(42).onCancel(cancelled.set(true)),
          size = 10
        )
        fiber <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.invalidate("hello")
        result <- fiber.joinWithNever.timeout(500.millis)
        didCancel <- cancelled.get
      yield (result, didCancel)

      run(test.map { case (result, didCancel) =>
        assert(result == 42)
        assert(didCancel == false)
      })
    }

    test("invalidate of an in-flight key clears the cache: subsequent gets re-fetch") {
      var calls = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.sleep(50.millis) >> IO { calls += 1; calls },
          size = 10
        )
        fiber <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.invalidate("hello")
        first <- fiber.joinWithNever.timeout(500.millis)
        second <- cache.get("hello")
      yield (first, second)

      run(test.map { case (first, second) =>
        assert(first == 1)
        assert(second == 2)
      })
    }

    test("invalidate of one key does not evict others") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        _ <- cache.get("hello")
        _ <- cache.get("world")
        _ <- cache.invalidate("hello")
        _ <- cache.get("world")
      yield ()

      run(test.map(_ => assert(callCount == 2)))
    }

    test("put stores value retrievable via get without calling fetch") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        _ <- cache.put("hello", 99)
        result <- cache.get("hello")
      yield result

      run(test.map { result =>
        assert(result == 99)
        assert(callCount == 0)
      })
    }

    test("put overwrites previously fetched value") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO.pure(key.length),
          size = 10
        )
        _ <- cache.get("hello")
        _ <- cache.put("hello", 99)
        result <- cache.get("hello")
      yield result

      run(test.map(result => assert(result == 99)))
    }

    test("failed in-flight fetch does not wipe out a newer put for the same key") {
      val error = new RuntimeException("boom") {
        override def fillInStackTrace() = this
      }
      var calls = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ =>
            IO.sleep(50.millis) >>
              IO {
                calls += 1
                if calls == 1 then throw error else 42
              },
          size = 10
        )
        fiber <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.put("hello", 99)
        _ <- fiber.join
        value <- cache.get("hello")
      yield value

      run(test.map { value =>
        assert(value == 99)
        assert(calls <= 1)
      })
    }

    test("put during an in-flight fetch wakes existing waiters with the put value") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.sleep(3.seconds).as(42),
          size = 10
        )
        fiber1 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        fiber2 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.put("hello", 99)
        value <- fiber2.joinWithNever.timeout(500.millis)
        _ <- fiber1.cancel
      yield value

      run(test.map(value => assert(value == 99)))
    }

    test("put during an in-flight fetch cancels the stale fetch") {
      val test = for
        cancelled <- IO.ref(false)
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.never[Int].onCancel(cancelled.set(true)),
          size = 10
        )
        fiber <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.put("hello", 99)
        _ <- fiber.joinWithNever.timeout(500.millis)
        didCancel <- cancelled.get.timeout(500.millis)
      yield didCancel

      run(test.map(didCancel => assert(didCancel == true)))
    }

    test("put does not affect other keys") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        _ <- cache.put("hello", 99)
        result <- cache.get("world")
      yield result

      run(test.map { result =>
        assert(result == 5)
        assert(callCount == 1)
      })
    }

    test("put then invalidate: subsequent get re-fetches") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        _ <- cache.put("hello", 99)
        _ <- cache.invalidate("hello")
        result <- cache.get("hello")
      yield result

      run(test.map { result =>
        assert(result == 5)
        assert(callCount == 1)
      })
    }

    test("concurrent gets after put all return put value without calling fetch") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        _ <- cache.put("hello", 99)
        results <- List.fill(5)(cache.get("hello")).parSequence
      yield results

      run(test.map { results =>
        assert(callCount == 0)
        assert(results == List.fill(5)(99))
      })
    }

    test("invalidateAll on empty cache is a no-op") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO.pure(key.length),
          size = 10
        )
        _ <- cache.invalidateAll
      yield ()

      run(test)
    }

    test("invalidateAll evicts all keys: subsequent gets re-fetch") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        _ <- cache.get("hello")
        _ <- cache.get("world")
        _ <- cache.invalidateAll
        _ <- cache.get("hello")
        _ <- cache.get("world")
      yield ()

      run(test.map(_ => assert(callCount == 4)))
    }

    test("invalidateAll lets in-flight waiters and the original getter receive their fetched values") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.sleep(100.millis).as(42),
          size = 10
        )
        fiber1 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        fiber2 <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.invalidateAll
        waiter <- fiber2.joinWithNever.timeout(500.millis)
        original <- fiber1.joinWithNever.timeout(500.millis)
      yield (original, waiter)

      run(test.map { case (original, waiter) =>
        assert(original == 42)
        assert(waiter == 42)
      })
    }

    test("invalidateAll does not cancel in-flight fetches") {
      val test = for
        cancelled <- IO.ref(false)
        cache <- MurCache.simple[IO, String, Int](
          fetch =
            _ => IO.sleep(100.millis).as(42).onCancel(cancelled.set(true)),
          size = 10
        )
        fiber <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.invalidateAll
        result <- fiber.joinWithNever.timeout(500.millis)
        didCancel <- cancelled.get
      yield (result, didCancel)

      run(test.map { case (result, didCancel) =>
        assert(result == 42)
        assert(didCancel == false)
      })
    }

    test("invalidateAll of an in-flight key clears the cache: subsequent gets re-fetch") {
      var calls = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO.sleep(50.millis) >> IO { calls += 1; calls },
          size = 10
        )
        fiber <- cache.get("hello").start
        _ <- IO.sleep(10.millis)
        _ <- cache.invalidateAll
        first <- fiber.joinWithNever.timeout(500.millis)
        second <- cache.get("hello")
      yield (first, second)

      run(test.map { case (first, second) =>
        assert(first == 1)
        assert(second == 2)
      })
    }

    test("invalidateAll evicts put values: subsequent gets re-fetch") {
      var callCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO { callCount += 1; key.length },
          size = 10
        )
        _ <- cache.put("hello", 99)
        _ <- cache.invalidateAll
        result <- cache.get("hello")
      yield result

      run(test.map { result =>
        assert(result == 5)
        assert(callCount == 1)
      })
    }

    test("evicted key is re-cached after re-fetch") {
      var fetchCounts = Map.empty[String, Int]

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key =>
            IO {
              fetchCounts =
                fetchCounts.updated(key, fetchCounts.getOrElse(key, 0) + 1)
              key.length
            },
          size = 2
        )
        _ <- cache.get("a")
        _ <- cache.get("b")
        _ <- cache.get("c")
        _ <- cache.get("a")
        _ <- cache.get("a")
      yield ()

      run(test.map(_ => assert(fetchCounts.getOrElse("a", 0) == 2)))
    }

    test("oldest key is invalidated when cache exceeds size") {
      var fetchCounts = Map.empty[String, Int]

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key =>
            IO {
              fetchCounts =
                fetchCounts.updated(key, fetchCounts.getOrElse(key, 0) + 1)
              key.length
            },
          size = 2
        )
        _ <- cache.get("a")
        _ <- cache.get("b")
        _ <- cache.get("c")
        _ <- cache.get("a")
      yield ()

      run(test.map { _ =>
        assert(fetchCounts("a") == 2)
        assert(fetchCounts("b") == 1)
        assert(fetchCounts("c") == 1)
      })
    }

    test("accessing a cached middle key does not corrupt LRU eviction order") {
      var fetchCounts = Map.empty[String, Int]

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key =>
            IO {
              fetchCounts =
                fetchCounts.updated(key, fetchCounts.getOrElse(key, 0) + 1)
              key.length
            },
          size = 3
        )
        _ <- cache.get("a")
        _ <- cache.get("b")
        _ <- cache.get("c")
        _ <- cache.get("b")
        _ <- cache.get("d")
        _ <- cache.get("a")
      yield ()

      run(test.map { _ =>
        assert(fetchCounts("a") == 2)
        assert(fetchCounts("b") == 1)
        assert(fetchCounts("c") == 1)
        assert(fetchCounts("d") == 1)
      })
    }

    test("invalidating the new LRU tail after an eviction does not crash") {
      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key => IO.pure(key.length),
          size = 3
        )
        _ <- cache.get("a")
        _ <- cache.get("b")
        _ <- cache.get("c")
        _ <- cache.get("d")
        wasPresent <- cache.invalidate("b")
      yield wasPresent

      run(test.map(wasPresent => assert(wasPresent == true)))
    }

    test("eviction does not corrupt the previous head's LRU position") {
      var fetchCounts = Map.empty[String, Int]

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key =>
            IO {
              fetchCounts =
                fetchCounts.updated(key, fetchCounts.getOrElse(key, 0) + 1)
              key.length
            },
          size = 3
        )
        _ <- cache.get("a")
        _ <- cache.get("b")
        _ <- cache.get("c")
        _ <- cache.get("d")
        _ <- cache.get("c")
        _ <- cache.get("e")
        _ <- cache.get("b")
      yield ()

      run(test.map { _ =>
        assert(fetchCounts("b") == 2)
        assert(fetchCounts("d") == 1)
      })
    }

    test("promoting a node adjacent to the head does not orphan the old head") {
      var fetchCounts = Map.empty[String, Int]

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key =>
            IO {
              fetchCounts =
                fetchCounts.updated(key, fetchCounts.getOrElse(key, 0) + 1)
              key.length
            },
          size = 4
        )
        _ <- cache.get("a")
        _ <- cache.get("b")
        _ <- cache.get("c")
        _ <- cache.get("d")
        _ <- cache.get("c")
        _ <- cache.get("d")
        _ <- cache.get("e")
        _ <- cache.get("f")
        _ <- cache.get("g")
        _ <- cache.get("h")
        _ <- cache.get("c")
      yield ()

      run(test.map(_ => assert(fetchCounts("c") == 2)))
    }

    test("put on an existing key in a full cache does not evict or re-fetch") {
      var fetchCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO { fetchCount += 1; 0 },
          size = 2
        )
        _ <- cache.put("a", 1)
        _ <- cache.put("b", 2)
        _ <- cache.put("a", 99)
        result <- cache.get("a")
      yield result

      run(test.map { result =>
        assert(result == 99)
        assert(fetchCount == 0)
      })
    }

    test("put on an existing key updates its LRU position") {
      var fetchCount = 0

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = _ => IO { fetchCount += 1; 0 },
          size = 2
        )
        _ <- cache.put("a", 1)
        _ <- cache.put("b", 2)
        _ <- cache.put("a", 99)
        _ <- cache.get("c")
        result <- cache.get("a")
      yield result

      run(test.map { result =>
        assert(result == 99)
        assert(fetchCount == 1)
      })
    }

    test("LRU eviction order is preserved across multiple evictions") {
      var fetchCounts = Map.empty[String, Int]

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key =>
            IO {
              fetchCounts =
                fetchCounts.updated(key, fetchCounts.getOrElse(key, 0) + 1)
              key.length
            },
          size = 2
        )
        _ <- cache.get("a")
        _ <- cache.get("b")
        _ <- cache.get("c")
        _ <- cache.get("d")
        _ <- cache.get("e")
        _ <- cache.get("c")
      yield ()

      run(test.map(_ => assert(fetchCounts("c") == 2)))
    }

    test("size-1 cache evicts its only entry when a new key is fetched") {
      var fetchCounts = Map.empty[String, Int]

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key =>
            IO {
              fetchCounts =
                fetchCounts.updated(key, fetchCounts.getOrElse(key, 0) + 1)
              key.length
            },
          size = 1
        )
        _ <- cache.get("a")
        _ <- cache.get("b")
        _ <- cache.get("a")
      yield ()

      run(test.map { _ =>
        assert(fetchCounts("a") == 2)
        assert(fetchCounts("b") == 1)
      })
    }

    test("evicting an in-flight key does not cancel its original fetch") {
      var fetchCounts = Map.empty[String, Int]

      val test = for
        cache <- MurCache.simple[IO, String, Int](
          fetch = key =>
            IO.sleep(if key == "a" then 50.millis else Duration.Zero) >>
              IO {
                fetchCounts =
                  fetchCounts.updated(key, fetchCounts.getOrElse(key, 0) + 1)
                key.length
              },
          size = 1
        )
        fiber <- cache.get("a").start
        _ <- IO.sleep(10.millis)
        _ <- cache.get("b")
        original <- fiber.joinWithNever.timeout(500.millis)
        refetched <- cache.get("a")
      yield (original, refetched)

      run(test.map { case (original, refetched) =>
        assert((original, refetched) == (1, 1))
        assert(fetchCounts("a") == 2)
        assert(fetchCounts("b") == 1)
      })
    }

    test("invalidateAll does not affect independent cache instances") {
      var callCount = 0
      val fetch: String => IO[Int] = key => IO { callCount += 1; key.length }

      val test = for
        cache1 <- MurCache.simple[IO, String, Int](fetch, size = 10)
        cache2 <- MurCache.simple[IO, String, Int](fetch, size = 10)
        _ <- cache1.get("hello")
        _ <- cache2.get("hello")
        _ <- cache1.invalidateAll
        _ <- cache2.get("hello")
      yield ()

      run(test.map(_ => assert(callCount == 2)))
    }
  }
