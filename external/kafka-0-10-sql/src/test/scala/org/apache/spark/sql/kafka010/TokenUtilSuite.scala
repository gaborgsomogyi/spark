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

package org.apache.spark.sql.kafka010

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._

class TokenUtilSuite extends SparkFunSuite with BeforeAndAfterEach {
  private val bootStrapServers = "127.0.0.1:0"
  private val plainSecurityProtocol = "SASL_PLAINTEXT"
  private val sslSecurityProtocol = "SASL_SSL"
  private val trustStoreLocation = "/path/to/trustStore"
  private val trustStorePassword = "secret"
  private val keytab = "/path/to/keytab"
  private val kerberosServiceName = "kafka"
  private val principal = "user@domain.com"

  private var sparkConf: SparkConf = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    sparkConf = new SparkConf()
  }

  test("createAdminClientProperties without bootstrap servers should throw exception") {
    val thrown = intercept[IllegalArgumentException] {
      TokenUtil.createAdminClientProperties(sparkConf)
    }
    assert(thrown.getMessage contains
      "Tried to obtain kafka delegation token but bootstrap servers not configured.")
  }

  test("createAdminClientProperties without SSL protocol should not take over truststore config") {
    sparkConf.set(KAFKA_BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(KAFKA_SECURITY_PROTOCOL, plainSecurityProtocol)
    sparkConf.set(KAFKA_TRUSTSTORE_LOCATION, trustStoreLocation)
    sparkConf.set(KAFKA_TRUSTSTORE_PASSWORD, trustStoreLocation)

    val adminClientProperties = TokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      .equals(bootStrapServers))
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      .equals(plainSecurityProtocol))
    assert(!adminClientProperties.containsKey("ssl.truststore.location"))
    assert(!adminClientProperties.containsKey("ssl.truststore.password"))
  }

  test("createAdminClientProperties with SSL protocol should take over truststore config") {
    sparkConf.set(KAFKA_BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(KAFKA_SECURITY_PROTOCOL, sslSecurityProtocol)
    sparkConf.set(KAFKA_TRUSTSTORE_LOCATION, trustStoreLocation)
    sparkConf.set(KAFKA_TRUSTSTORE_PASSWORD, trustStorePassword)

    val adminClientProperties = TokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      .equals(bootStrapServers))
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      .equals(sslSecurityProtocol))
    assert(adminClientProperties.get("ssl.truststore.location").equals(trustStoreLocation))
    assert(adminClientProperties.get("ssl.truststore.password").equals(trustStorePassword))
  }

  test("createAdminClientProperties without keytab should not set dynamic jaas config") {
    sparkConf.set(KAFKA_BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(KAFKA_SECURITY_PROTOCOL, sslSecurityProtocol)

    val adminClientProperties = TokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      .equals(bootStrapServers))
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      .equals(sslSecurityProtocol))
    assert(!adminClientProperties.containsKey(SaslConfigs.SASL_MECHANISM))
    assert(!adminClientProperties.containsKey(SaslConfigs.SASL_JAAS_CONFIG))
  }

  test("createAdminClientProperties with keytab should set dynamic jaas config") {
    sparkConf.set(KAFKA_BOOTSTRAP_SERVERS, bootStrapServers)
    sparkConf.set(KAFKA_SECURITY_PROTOCOL, sslSecurityProtocol)
    sparkConf.set(KEYTAB, keytab)
    sparkConf.set(KAFKA_KERBEROS_SERVICE_NAME, kerberosServiceName)
    sparkConf.set(PRINCIPAL, principal)

    val adminClientProperties = TokenUtil.createAdminClientProperties(sparkConf)

    assert(adminClientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
      .equals(bootStrapServers))
    assert(adminClientProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)
      .equals(sslSecurityProtocol))
    assert(adminClientProperties.containsKey(SaslConfigs.SASL_MECHANISM))
    assert(adminClientProperties.containsKey(SaslConfigs.SASL_JAAS_CONFIG))
  }
}
