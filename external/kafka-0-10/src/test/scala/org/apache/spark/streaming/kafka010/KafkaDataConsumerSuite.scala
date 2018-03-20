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

import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalatest.BeforeAndAfterAll

import org.apache.spark._

class KafkaDataConsumerSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var testUtils: KafkaTestUtils = _

  override def beforeAll {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    super.afterAll()
  }

  test("concurrent use of KafkaDataConsumer") {
    KafkaDataConsumer.init(16, 64, 0.75f)

    val topic = "topic" + Random.nextInt()
    val data = (1 to 1000).map(_.toString)
    val topicPartition = new TopicPartition(topic, 0)
    testUtils.createTopic(topic)
    testUtils.sendMessages(topic, data.toArray)

    val groupId = "groupId"
    val kafkaParams = Map[String, Object](
      GROUP_ID_CONFIG -> groupId,
      BOOTSTRAP_SERVERS_CONFIG -> testUtils.brokerAddress,
      KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val numThreads = 100
    val numConsumerUsages = 500

    @volatile var error: Throwable = null

    def consume(i: Int): Unit = {
      val useCache = Random.nextBoolean
      val taskContext = if (Random.nextBoolean) {
        new TaskContextImpl(0, 0, 0, 0, attemptNumber = Random.nextInt(2), null, null, null)
      } else {
        null
      }
      val consumer = KafkaDataConsumer.acquire[Array[Byte], Array[Byte]](
        groupId, topicPartition, kafkaParams.asJava, taskContext, useCache)
      try {
        val rcvd = 0 until data.length map { offset =>
          val bytes = consumer.get(offset, 10000).value()
          new String(bytes)
        }
        assert(rcvd == data)
      } catch {
        case e: Throwable =>
          error = e
          throw e
      } finally {
        consumer.release()
      }
    }

    val threadpool = Executors.newFixedThreadPool(numThreads)
    try {
      val futures = (1 to numConsumerUsages).map { i =>
        threadpool.submit(new Runnable {
          override def run(): Unit = { consume(i) }
        })
      }
      futures.foreach(_.get(1, TimeUnit.MINUTES))
      assert(error == null)
    } finally {
      threadpool.shutdown()
    }
  }
}
