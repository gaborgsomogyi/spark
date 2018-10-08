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

import java.util.UUID

import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.{KAFKA_KERBEROS_SERVICE_NAME, KEYTAB, PRINCIPAL}
import org.apache.spark.sql.kafka010.TokenUtil.KafkaDelegationTokenIdentifier

class KafkaSecurityHelperSuite extends SparkFunSuite with BeforeAndAfterEach {
  private val keytab = "/path/to/keytab"
  private val kerberosServiceName = "kafka"
  private val principal = "user@domain.com"
  private val tokenId = "tokenId" + UUID.randomUUID().toString
  private val tokenPassword = "tokenPassword" + UUID.randomUUID().toString

  private var sparkConf: SparkConf = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    sparkConf = new SparkConf()
  }

  private def addTokenToUGI: Unit = {
    val token = new Token[KafkaDelegationTokenIdentifier](
      tokenId.getBytes,
      tokenPassword.getBytes,
      TokenUtil.TOKEN_KIND,
      TokenUtil.TOKEN_SERVICE
    )
    val creds = new Credentials()
    creds.addToken(TokenUtil.TOKEN_SERVICE, token)
    UserGroupInformation.getCurrentUser.addCredentials(creds)
  }

  private def resetUGI: Unit = {
    UserGroupInformation.setLoginUser(null)
  }

  test("getKeytabJaasParams without keytab should return None") {
    val jaasParams = KafkaSecurityHelper.getKeytabJaasParams(sparkConf)
    assert(!jaasParams.isDefined)
  }

  test("getKeytabJaasParams with keytab no service should throw exception") {
    sparkConf.set(KEYTAB, keytab)

    val thrown = intercept[IllegalArgumentException] {
      KafkaSecurityHelper.getKeytabJaasParams(sparkConf)
    }

    assert(thrown.getMessage contains "Kerberos service name must be defined")
  }

  test("getKeytabJaasParams with keytab no principal should throw exception") {
    sparkConf.set(KEYTAB, keytab)
    sparkConf.set(KAFKA_KERBEROS_SERVICE_NAME, kerberosServiceName)

    val thrown = intercept[IllegalArgumentException] {
      KafkaSecurityHelper.getKeytabJaasParams(sparkConf)
    }

    assert(thrown.getMessage contains "Principal must be defined")
  }

  test("getKeytabJaasParams with keytab should return kerberos module") {
    sparkConf.set(KEYTAB, keytab)
    sparkConf.set(KAFKA_KERBEROS_SERVICE_NAME, kerberosServiceName)
    sparkConf.set(PRINCIPAL, principal)

    val jaasParams = KafkaSecurityHelper.getKeytabJaasParams(sparkConf)

    assert(jaasParams.get.contains("Krb5LoginModule"))
  }

  test("getTokenJaasParams without token should return None") {
    val jaasParams = KafkaSecurityHelper.getTokenJaasParams(sparkConf)
    assert(!jaasParams.isDefined)
  }

  test("getTokenJaasParams with token no service should throw exception") {
    try {
      addTokenToUGI

      val thrown = intercept[IllegalArgumentException] {
        KafkaSecurityHelper.getTokenJaasParams(sparkConf)
      }

      assert(thrown.getMessage contains "Kerberos service name must be defined")
    } finally {
      resetUGI
    }
  }

  test("getTokenJaasParams with token should return scram module") {
    try {
      addTokenToUGI
      sparkConf.set(KAFKA_KERBEROS_SERVICE_NAME, kerberosServiceName)

      val jaasParams = KafkaSecurityHelper.getTokenJaasParams(sparkConf)

      assert(jaasParams.get.contains("ScramLoginModule"))
      assert(jaasParams.get.contains(tokenId))
      assert(jaasParams.get.contains(tokenPassword))
    } finally {
      resetUGI
    }
  }
}
