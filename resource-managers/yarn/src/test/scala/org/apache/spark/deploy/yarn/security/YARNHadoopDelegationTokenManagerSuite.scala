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

package org.apache.spark.deploy.yarn.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials

import org.apache.spark.{SparkConf, SparkFunSuite}

class YARNHadoopDelegationTokenManagerSuite extends SparkFunSuite {
  private var credentialManager: YARNHadoopDelegationTokenManager = null
  private var sparkConf: SparkConf = null
  private var hadoopConf: Configuration = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkConf = new SparkConf()
    hadoopConf = new Configuration()
  }

  test("Correctly loads credential providers") {
    ExceptionThrowingServiceCredentialProvider.constructed = false
    credentialManager = new YARNHadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(credentialManager.isProviderLoaded("yarn-test"))
    // This checks that providers are loaded independently and they have no effect on each other
    assert(ExceptionThrowingServiceCredentialProvider.constructed)
    assert(!credentialManager.isProviderLoaded("throw"))
  }
}

private class YARNTestCredentialProvider extends ServiceCredentialProvider {
  override def serviceName: String = "yarn-test"

  override def credentialsRequired(conf: Configuration): Boolean = true

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = None
}

private class ExceptionThrowingServiceCredentialProvider extends ServiceCredentialProvider {
  ExceptionThrowingServiceCredentialProvider.constructed = true
  throw new IllegalArgumentException

  override def serviceName: String = "throw"

  override def credentialsRequired(conf: Configuration): Boolean = true

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = None
}

private object ExceptionThrowingServiceCredentialProvider {
  var constructed = false
}
