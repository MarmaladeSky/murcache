/*
 * Copyright 2026 David Akermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package digital.junkie.murcache

import cats.effect.syntax.monadCancel.*
import cats.syntax.functor.*
import cats.syntax.flatMap.*
import cats.syntax.applicativeError.*
import cats.syntax.either.*
import cats.effect.kernel.Ref
import cats.effect.kernel.Concurrent
import cats.effect.kernel.Deferred
import cats.effect.kernel.Fiber
import scala.concurrent.CancellationException

trait MurCache[F[_], K, V] {

  def get(key: K, fetch: K => F[V]): F[V]

  def put(key: K, value: V): F[Unit]

  def invalidate(key: K): F[Boolean]

  def invalidateAll: F[Unit]

  def size: F[Int]

}

trait MurCacheWithDefault[F[_], K, V] extends MurCache[F, K, V] {

  def get(key: K): F[V]

}

object MurCache {

  def withDefault[F[_], K, V](
      default: K => F[V],
      m: MurCache[F, K, V]
  ): MurCacheWithDefault[F, K, V] = new MurCacheWithDefault[F, K, V] {

    def get(key: K, fetch: K => F[V]): F[V] = m.get(key, fetch)

    def put(key: K, value: V): F[Unit] = m.put(key, value)

    def invalidate(key: K): F[Boolean] = m.invalidate(key)

    def invalidateAll: F[Unit] = m.invalidateAll

    def size: F[Int] = m.size

    def get(key: K): F[V] = m.get(key, default)

  }

  def noop[F[_], K, V]()(implicit
      F: Concurrent[F]
  ): MurCache[F, K, V] = new MurCache[F, K, V] {

    def get(key: K, fetch: K => F[V]): F[V] = fetch(key)

    def put(key: K, value: V): F[Unit] = F.unit

    def invalidate(key: K): F[Boolean] = F.pure(false)

    def invalidateAll: F[Unit] = F.unit

    def size = F.pure(0)

  }

  def noop[F[_], K, V](default: K => F[V])(implicit
      F: Concurrent[F]
  ): MurCacheWithDefault[F, K, V] = withDefault(default, noop[F, K, V]())

  private case class UnsafeLRU[F[_], K, V](
      map: Map[K, Deferred[F, Either[Throwable, V]]],
      list: Map[K, (prev: Option[K], next: Option[K])],
      head: Option[K],
      tail: Option[K]
  ) {

    def size: Int = map.size

    def contains(key: K): Boolean = map.contains(key)

    def -(key: K): UnsafeLRU[F, K, V] = {
      val (newList, newHead, newTail) = list.get(key) match {
        // head
        case Some((prev = Some(prev), next = None)) =>
          val prevNode = list(prev)
          val l = (list + (prev -> (prev = prevNode.prev, next = None))) - key
          val h = prev
          val t = tail
          (l, Some(h), t)

        // tail
        case Some((prev = None, next = Some(next))) =>
          val nextNode = list(next)
          val l = (list + (next -> (prev = None, next = nextNode.next))) - key
          val h = head
          val t = next
          (l, h, Some(t))

        // middle
        case Some((Some(prev), Some(next))) =>
          val prevNode = list(prev)
          val nextNode = list(next)

          val newPrevNode = (prev = prevNode.prev, next = Some(next))
          val newNextNode = (prev = Some(prev), next = nextNode.next)
          val l = ((list + (prev -> newPrevNode)) + (next -> newNextNode)) - key
          (l, head, tail)

        // single
        case Some((None, None)) =>
          (list - key, None, None)

        case None =>
          (list, head, tail)
      }

      UnsafeLRU(
        map - key,
        newList,
        newHead,
        newTail
      )
    }

    def +(
        key: K,
        value: Deferred[F, Either[Throwable, V]],
        pushOut: Boolean
    ): UnsafeLRU[F, K, V] = {
      if (pushOut) {
        val newMap = map + (key -> value) - tail.get
        list(tail.get).next match {
          case None =>
            // single-element cache: replace the only entry
            UnsafeLRU(
              map = newMap,
              list = Map(key -> (prev = None, next = None)),
              head = Some(key),
              tail = Some(key)
            )
          case Some(newTailKey) =>
            val newList =
              if (newTailKey == head.get)
                // 2-element cache: newTailKey IS the old head
                list - tail.get
                  + (newTailKey -> (prev = None, next = Some(key)))
                  + (key -> (prev = Some(newTailKey), next = None))
              else
                // 3+ element cache
                list - tail.get
                  + (newTailKey -> (prev = None, next = list(newTailKey).next))
                  + (head.get -> (prev = list(head.get).prev, next = Some(key)))
                  + (key -> (prev = head, next = None))
            UnsafeLRU(
              map = newMap,
              list = newList,
              head = Some(key),
              tail = Some(newTailKey)
            )
        }
      } else { // cache size allow put without eviction
        if (contains(key)) {
          val promoted = touch(key)
          promoted.copy(map = promoted.map + (key -> value))
        } else {
          UnsafeLRU(
            map = map + (key -> value),
            list = head match {
              case Some(head) =>
                val oldHead = head -> (prev = list(head).prev, next = Some(key))
                val newHead = key -> (prev = Some(head), next = None)
                list + oldHead + newHead
              case None => // empty
                Map(key -> (prev = None, next = None))
            },
            head = Some(key),
            tail = Some(tail.getOrElse(key))
          )
        }
      }
    }

    def touch(key: K): UnsafeLRU[F, K, V] = {
      list.get(key) match {
        case Some((prev = Some(_), next = None)) => this // already head
        case Some((None, None))                  => this // single element
        case None                                => this // not in cache

        case Some((prev = None, next = Some(nextKey))) =>
          // tail: promote to head
          if (nextKey == head.get)
            // 2-element cache: nextKey IS the old head
            UnsafeLRU(
              map,
              list + (nextKey -> (prev = None, next = Some(key)))
                + (key -> (prev = Some(nextKey), next = None)),
              tail = Some(nextKey),
              head = Some(key)
            )
          else
            // 3+ elements
            UnsafeLRU(
              map,
              list + (nextKey -> (prev = None, next = list(nextKey).next))
                + (head.get -> (prev = list(head.get).prev, next = Some(key)))
                + (key -> (prev = head, next = None)),
              tail = Some(nextKey),
              head = Some(key)
            )

        case Some((Some(prevKey), Some(nextKey))) =>
          // middle: promote to head
          if (nextKey == head.get)
            // adjacent to head: nextKey and head are the same node
            UnsafeLRU(
              map,
              list + (prevKey -> (
                prev = list(prevKey).prev,
                next = Some(nextKey)
              ))
                + (nextKey -> (prev = Some(prevKey), next = Some(key)))
                + (key -> (prev = Some(nextKey), next = None)),
              tail = tail,
              head = Some(key)
            )
          else
            // general case
            UnsafeLRU(
              map,
              list + (prevKey -> (
                prev = list(prevKey).prev,
                next = Some(nextKey)
              ))
                + (nextKey -> (prev = Some(prevKey), next = list(nextKey).next))
                + (head.get -> (prev = list(head.get).prev, next = Some(key)))
                + (key -> (prev = head, next = None)),
              tail = tail,
              head = Some(key)
            )
      }
    }

    def get(key: K): Option[Deferred[F, Either[Throwable, V]]] = {
      map.get(key)
    }
  }

  def simple[F[_], K, V](
      maxSize: Int
  )(implicit F: Concurrent[F]): F[MurCache[F, K, V]] = {
    if (maxSize <= 0) {
      F.pure { noop() }
    } else {
      Ref
        .of[F, UnsafeLRU[F, K, V]] {
          UnsafeLRU[F, K, V](Map.empty, Map.empty, None, None)
        }
        .map { impl(maxSize, _) }
    }
  }

  def simple[F[_], K, V](
      default: K => F[V],
      maxSize: Int
  )(implicit F: Concurrent[F]): F[MurCacheWithDefault[F, K, V]] = {
    if (maxSize <= 0) {
      F.pure { withDefault(default, noop()) }
    } else {
      Ref
        .of[F, UnsafeLRU[F, K, V]] {
          UnsafeLRU[F, K, V](Map.empty, Map.empty, None, None)
        }
        .map { underlying => withDefault(default, impl(maxSize, underlying)) }
    }
  }

  private def impl[F[_], K, V](
      maxSize: Int,
      r: Ref[F, UnsafeLRU[F, K, V]]
  )(implicit
      F: Concurrent[F]
  ): MurCache[F, K, V] = new MurCache[F, K, V] {

    def cancelError = new CancellationException("task cancelled")

    def removeIfCurrent(
        key: K,
        expected: Deferred[F, Either[Throwable, V]]
    ): F[Unit] =
      r.update { u =>
        u.get(key) match {
          case Some(current) if current eq expected => u - key
          case _                                    => u
        }
      }

    def get(key: K, fetch: K => F[V]): F[V] = r.get.flatMap { m =>
      m.get(key) match {
        case Some(d) => // fast path
          r.flatModifyFull { (_, u) =>
            (u.touch(key), d.get.flatMap(_.liftTo[F]))
          }
        case None =>
          Deferred[F, Either[Throwable, V]].flatMap { newD =>
            r
              // returns fetch fiber if we have one and the related Deferred
              .flatModify { u =>
                u.get(key) match
                  case Some(d) =>
                    (u, F.pure((d, Option.empty[Fiber[F, Throwable, Unit]])))
                  case None =>
                    val runFetch = fetch(key).attempt
                      .flatTap(newD.complete)
                      .flatTap { x =>
                        if (x.isLeft) removeIfCurrent(key, newD)
                        else F.unit
                      }
                      .onCancel(
                        removeIfCurrent(key, newD) >>
                          newD
                            .complete(Left(cancelError))
                            .void
                      )
                      .void
                      .handleErrorWith {
                        case _: CancellationException => F.unit
                        case e                        => F.raiseError(e)
                      }

                    val effect = F
                      .start(runFetch)
                      .map { f => (newD, Option(f)) }

                    (
                      u + (key, newD, !u.contains(key) && u.size >= maxSize),
                      effect
                    )
              }
              // cancel fetch fiber if we got the Deferred completed
              .flatMap { (d, fetchFiber) =>
                val await = d.get.flatMap(_.liftTo[F])
                fetchFiber match {
                  case Some(fiber) =>
                    await
                      .flatTap(_ => fiber.cancel)
                      .onError(_ => fiber.cancel)
                      .onCancel(fiber.cancel)
                  case None => await

                }
              }
          }
      }
    }

    def put(key: K, value: V): F[Unit] = {
      Deferred[F, Either[Throwable, V]].flatMap { newD =>
        r.flatModifyFull { (_, u) =>
          val current = u.get(key)
          val updated = u + (key, newD, !u.contains(key) && u.size >= maxSize)
          val publish = {
            val result = value.asRight[Throwable]
            val wakeCurrent = current match {
              case Some(existing) => existing.complete(result).void
              case _              => F.unit
            }
            newD.complete(result).void >> wakeCurrent
          }
          (updated, publish)
        }
      }
    }

    def invalidate(key: K): F[Boolean] =
      r.modify { m => (m.-(key), m.contains(key)) }

    def invalidateAll: F[Unit] =
      r.getAndSet(UnsafeLRU(Map.empty, Map.empty, None, None)).void

    def size: F[Int] = r.get.map(_.size)

  }

}
