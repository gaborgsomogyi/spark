/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.kafka010

import java.{util => ju}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

private[kafka010] sealed trait KafkaDataConsumer[K, V] {
  /**
   * Get the record for the given offset if available.
   *
   * @param offset         the offset to fetch.
   * @param pollTimeoutMs  timeout in milliseconds to poll data from Kafka.
   */
  def get(offset: Long, pollTimeoutMs: Long): ConsumerRecord[K, V] = {
    internalConsumer.get(offset, pollTimeoutMs)
  }

  /**
   * Start a batch on a compacted topic
   *
   * @param offset         the offset to fetch.
   * @param pollTimeoutMs  timeout in milliseconds to poll data from Kafka.
   */
  def compactedStart(offset: Long, pollTimeoutMs: Long): Unit = {
    internalConsumer.compactedStart(offset, pollTimeoutMs)
  }

  /**
   * Get the next record in the batch from a compacted topic.
   * Assumes compactedStart has been called first, and ignores gaps.
   *
   * @param pollTimeoutMs  timeout in milliseconds to poll data from Kafka.
   */
  def compactedNext(pollTimeoutMs: Long): ConsumerRecord[K, V] = {
    internalConsumer.compactedNext(pollTimeoutMs)
  }

  /**
   * Rewind to previous record in the batch from a compacted topic.
   *
   * @throws NoSuchElementException if no previous element
   */
  def compactedPrevious(): ConsumerRecord[K, V] = {
    internalConsumer.compactedPrevious()
  }

  /**
   * Release this consumer from being further used. Depending on its implementation,
   * this consumer will be either finalized, or reset for reuse later.
   */
  def release(): Unit

  /** Reference to the internal implementation that this wrapper delegates to */
  protected def internalConsumer: InternalKafkaConsumer[K, V]
}


/**
 * A wrapper around Kafka's KafkaConsumer.
 * This is not for direct use outside this file.
 */
private[kafka010]
class InternalKafkaConsumer[K, V](
  val groupId: String,
  val topicPartition: TopicPartition,
  val kafkaParams: ju.Map[String, Object]) extends Logging {

  require(groupId == kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG),
    "groupId used for cache key must match the groupId in kafkaParams")

  @volatile private var consumer = createConsumer

  /** indicates whether this consumer is in use or not */
  @volatile var inUse = true

  /** indicate whether this consumer is going to be stopped in the next release */
  @volatile var markedForClose = false

  // TODO if the buffer was kept around as a random-access structure,
  // could possibly optimize re-calculating of an RDD in the same batch
  @volatile private var buffer = ju.Collections.emptyListIterator[ConsumerRecord[K, V]]()
  @volatile private var nextOffset = InternalKafkaConsumer.UNKNOWN_OFFSET

  /** Create a KafkaConsumer to fetch records for `topicPartition` */
  private def createConsumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer[K, V](kafkaParams)
    val tps = new ju.ArrayList[TopicPartition]()
    tps.add(topicPartition)
    c.assign(tps)
    c
  }

  def close(): Unit = consumer.close()

  /**
   * Get the record for the given offset, waiting up to timeout ms if IO is necessary.
   * Sequential forward access will use buffers, but random access will be horribly inefficient.
   */
  def get(offset: Long, timeout: Long): ConsumerRecord[K, V] = {
    logDebug(s"Get $groupId $topicPartition nextOffset $nextOffset requested $offset")
    if (offset != nextOffset) {
      logInfo(s"Initial fetch for $groupId $topicPartition $offset")
      seek(offset)
      poll(timeout)
    }

    if (!buffer.hasNext()) { poll(timeout) }
    require(buffer.hasNext(),
      s"Failed to get records for $groupId $topicPartition $offset after polling for $timeout")
    var record = buffer.next()

    if (record.offset != offset) {
      logInfo(s"Buffer miss for $groupId $topicPartition $offset")
      seek(offset)
      poll(timeout)
      require(buffer.hasNext(),
        s"Failed to get records for $groupId $topicPartition $offset after polling for $timeout")
      record = buffer.next()
      require(record.offset == offset,
        s"Got wrong record for $groupId $topicPartition even after seeking to offset $offset " +
          s"got offset ${record.offset} instead. If this is a compacted topic, consider enabling " +
          "spark.streaming.kafka.allowNonConsecutiveOffsets"
      )
    }

    nextOffset = offset + 1
    record
  }

  /**
   * Start a batch on a compacted topic
   */
  def compactedStart(offset: Long, pollTimeoutMs: Long): Unit = {
    logDebug(s"compacted start $groupId $topicPartition starting $offset")
    // This seek may not be necessary, but it's hard to tell due to gaps in compacted topics
    if (offset != nextOffset) {
      logInfo(s"Initial fetch for compacted $groupId $topicPartition $offset")
      seek(offset)
      poll(pollTimeoutMs)
    }
  }

  /**
   * Get the next record in the batch from a compacted topic.
   * Assumes compactedStart has been called first, and ignores gaps.
   */
  def compactedNext(pollTimeoutMs: Long): ConsumerRecord[K, V] = {
    if (!buffer.hasNext()) {
      poll(pollTimeoutMs)
    }
    require(buffer.hasNext(),
      s"Failed to get records for compacted $groupId $topicPartition " +
        s"after polling for $pollTimeoutMs")
    val record = buffer.next()
    nextOffset = record.offset + 1
    record
  }

  /**
   * Rewind to previous record in the batch from a compacted topic.
   * @throws NoSuchElementException if no previous element
   */
  def compactedPrevious(): ConsumerRecord[K, V] = {
    buffer.previous()
  }

  private def seek(offset: Long): Unit = {
    logDebug(s"Seeking to $topicPartition $offset")
    consumer.seek(topicPartition, offset)
  }

  private def poll(timeout: Long): Unit = {
    val p = consumer.poll(timeout)
    val r = p.records(topicPartition)
    logDebug(s"Polled ${p.partitions()}  ${r.size}")
    buffer = r.listIterator
  }

}

private[kafka010]
object KafkaDataConsumer extends Logging {

  private case class CachedKafkaDataConsumer[K, V](internalConsumer: InternalKafkaConsumer[K, V])
    extends KafkaDataConsumer[K, V] {
    assert(internalConsumer.inUse) // make sure this has been set to true
    override def release(): Unit = { KafkaDataConsumer.release(internalConsumer) }
  }

  private case class NonCachedKafkaDataConsumer[K, V](internalConsumer: InternalKafkaConsumer[K, V])
    extends KafkaDataConsumer[K, V] {
    override def release(): Unit = { internalConsumer.close() }
  }

  private case class CacheKey(groupId: String, topicPartition: TopicPartition)

  // Don't want to depend on guava, don't want a cleanup thread, use a simple LinkedHashMap
  private var cache: ju.LinkedHashMap[CacheKey, InternalKafkaConsumer[_, _]] = null

  /**
   * Must be called before acquire, once per JVM, to configure the cache.
   * Further calls are ignored.
   * */
  def init(
      initialCapacity: Int,
      maxCapacity: Int,
      loadFactor: Float): Unit = synchronized {
    if (null == cache) {
      logInfo(s"Initializing cache $initialCapacity $maxCapacity $loadFactor")
      cache = new ju.LinkedHashMap[CacheKey, InternalKafkaConsumer[_, _]](
        initialCapacity, loadFactor, true) {
        override def removeEldestEntry(
          entry: ju.Map.Entry[CacheKey, InternalKafkaConsumer[_, _]]): Boolean = {
          if (this.size > maxCapacity) {
            try {
              entry.getValue.close()
            } catch {
              case x: KafkaException =>
                logError("Error closing oldest Kafka consumer", x)
            }
            true
          } else {
            false
          }
        }
      }
    }
  }

  /**
   * Get a cached consumer for groupId, assigned to topic and partition.
   * If matching consumer doesn't already exist, will be created using kafkaParams.
   * The returned consumer must be released explicitly using [[KafkaDataConsumer.release()]].
   *
   * Note: This method guarantees that the consumer returned is not currently in use by any one
   * else. Within this guarantee, this method will make a best effort attempt to re-use consumers by
   * caching them and tracking when they are in use.
   */
  def acquire[K, V](
      groupId: String,
      topicPartition: TopicPartition,
      kafkaParams: ju.Map[String, Object],
      context: TaskContext,
      useCache: Boolean): KafkaDataConsumer[K, V] = synchronized {
    val key = new CacheKey(groupId, topicPartition)
    val existingInternalConsumer = cache.get(key).asInstanceOf[InternalKafkaConsumer[K, V]]

    lazy val newInternalConsumer = new InternalKafkaConsumer[K, V](
      groupId, topicPartition, kafkaParams)

    if (context != null && context.attemptNumber >= 1) {
      // If this is reattempt at running the task, then invalidate cached consumer if any and
      // start with a new one.
      if (existingInternalConsumer != null) {
        // Consumer exists in cache. If its in use, mark it for closing later, or close it now.
        if (existingInternalConsumer.inUse) {
          existingInternalConsumer.markedForClose = true
        } else {
          existingInternalConsumer.close()
        }
      }
      cache.remove(key)  // Invalidate the cache in any case
      NonCachedKafkaDataConsumer(newInternalConsumer)

    } else if (!useCache) {
      // If consumer reuse turned off, then do not use it, return a new consumer
      NonCachedKafkaDataConsumer(newInternalConsumer)

    } else if (existingInternalConsumer == null) {
      // If consumer is not already cached, then put a new in the cache and return it
      cache.put(key, newInternalConsumer)
      newInternalConsumer.inUse = true
      CachedKafkaDataConsumer(newInternalConsumer)

    } else if (existingInternalConsumer.inUse) {
      // If consumer is already cached but is currently in use, then return a new consumer
      logInfo(s"Consumer for $groupId $topicPartition already in cache and used but a new " +
        s"request arrived. Creating a new non-cached consumer. Reading a topic's partition " +
        s"in different threads breaks ordering guarantees.")
      NonCachedKafkaDataConsumer(newInternalConsumer)

    } else {
      // If consumer is already cached and is currently not in use, then return that consumer
      existingInternalConsumer.inUse = true
      CachedKafkaDataConsumer(existingInternalConsumer)
    }
  }

  private def release[K, V](intConsumer: InternalKafkaConsumer[K, V]): Unit = synchronized {
    // Clear the consumer from the cache if this is indeed the consumer present in the cache
    val key = new CacheKey(intConsumer.groupId, intConsumer.topicPartition)
    val cachedIntConsumer = cache.get(key)
    if (intConsumer.eq(cachedIntConsumer)) {
      // The released consumer is the same object as the cached one.
      if (intConsumer.markedForClose) {
        intConsumer.close()
        cache.remove(key)
      } else {
        intConsumer.inUse = false
      }
    } else {
      // The released consumer is either not the same one as in the cache, or not in the cache
      // at all. This may happen if the cache was invalidate while this consumer was being used.
      // Just close this consumer.
      intConsumer.close()
      logInfo("Released a supposedly cached consumer that was not found in the cache")
    }
  }
}

private[kafka010] object InternalKafkaConsumer {
  private val UNKNOWN_OFFSET = -2L
}
