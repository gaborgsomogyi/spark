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

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.kafka.common.security.scram.ScramLoginModule

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

private[kafka010] object KafkaSecurityHelper extends Logging {
  def getKeytabJaasParams(sparkConf: SparkConf): Option[String] = {
    val keytab = sparkConf.get(KEYTAB)
    if (keytab.isDefined) {
      val serviceName = sparkConf.get(KAFKA_KERBEROS_SERVICE_NAME)
      require(serviceName.nonEmpty, "Kerberos service name must be defined")
      val principal = sparkConf.get(PRINCIPAL)
      require(principal.nonEmpty, "Principal must be defined")

      val params =
        s"""
        |${getKrb5LoginModuleName} required
        | useKeyTab=true
        | serviceName="${serviceName.get}"
        | keyTab="${keytab.get}"
        | principal="${principal.get}";
        """.stripMargin.replace("\n", "")
      logDebug(s"Krb JAAS params: $params")
      Some(params)
    } else {
      None
    }
  }

  private def getKrb5LoginModuleName(): String = {
    if (System.getProperty("java.vendor").contains("IBM")) {
      "com.ibm.security.auth.module.Krb5LoginModule"
    } else {
      "com.sun.security.auth.module.Krb5LoginModule"
    }
  }

  def getTokenJaasParams(sparkConf: SparkConf): Option[String] = {
    val token = UserGroupInformation.getCurrentUser().getCredentials.getToken(
      TokenUtil.TOKEN_SERVICE)
    if (token != null) {
      Some(getScramJaasParams(sparkConf, token))
    } else {
      None
    }
  }

  private def getScramJaasParams(
      sparkConf: SparkConf,
      token: Token[_ <: TokenIdentifier]): String = {
    val serviceName = sparkConf.get(KAFKA_KERBEROS_SERVICE_NAME)
    require(serviceName.isDefined, "Kerberos service name must be defined")
    val username = new String(token.getIdentifier)
    val password = new String(token.getPassword)

    val loginModuleName = classOf[ScramLoginModule].getName
    val params =
      s"""
      |$loginModuleName required
      | tokenauth=true
      | serviceName="${serviceName.get}"
      | username="$username"
      | password="$password";
      """.stripMargin.replace("\n", "")
    logDebug(s"Scram JAAS params: ${params.replaceAll("password=\".*\"", "password=\"[hidden]\"")}")

    params
  }
}
