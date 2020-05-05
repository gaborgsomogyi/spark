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

package org.apache.spark.sql.jdbc

import java.sql.DriverManager
import java.util.Properties
import javax.security.auth.login.Configuration

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.jdbc.connection.SecureConnectionProvider
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.DockerTest
import org.apache.spark.util.SecurityUtils

@DockerTest
class MultiKrbIntegrationSuite extends SharedSparkSession {
  protected val dockerIp = "example.com"

  val postgresPort = 5432
  val postgresKeytabFullPath = "/Users/gaborsomogyi/docker-kerberos/postgres_share/postgres.keytab"
  val postgresPrincipal = s"postgres/$dockerIp@EXAMPLE.COM"
  val postgresJdbcUrl =
    s"jdbc:postgresql://$dockerIp:$postgresPort/postgres?user=$postgresPrincipal&gsslib=gssapi"

  val mariaPort = 3306
  val mariaKeytabFullPath = "/Users/gaborsomogyi/docker-kerberos/mariadb_share/mariadb.keytab"
  val mariaPrincipal = s"mariadb/$dockerIp@EXAMPLE.COM"
  val mariaJdbcUrl = s"jdbc:mysql://$dockerIp:$mariaPort/mysql?user=$mariaPrincipal"

  test("Basic multi test") {
    SecurityUtils.setGlobalKrbDebug(true)

    val postgresConfig = new SecureConnectionProvider.JDBCConfiguration(
      Configuration.getConfiguration, "pgjdbc", postgresKeytabFullPath, postgresPrincipal)
    Configuration.setConfiguration(postgresConfig)
    val postgresConn = DriverManager.getConnection(postgresJdbcUrl, new Properties())
    postgresConn.prepareStatement("CREATE TABLE bar (c0 VARCHAR(8))").executeUpdate()
    postgresConn.prepareStatement("INSERT INTO bar VALUES ('hello')").executeUpdate()
    postgresConn.close()

    val mariaConfig = new SecureConnectionProvider.JDBCConfiguration(
      Configuration.getConfiguration, "Krb5ConnectorContext", mariaKeytabFullPath, mariaPrincipal)
    Configuration.setConfiguration(mariaConfig)
    val mariaConn = DriverManager.getConnection(mariaJdbcUrl, new Properties())
    mariaConn.prepareStatement("CREATE TABLE bar (c0 VARCHAR(8))").executeUpdate()
    mariaConn.prepareStatement("INSERT INTO bar VALUES ('hello')").executeUpdate()
    mariaConn.close()

    // This makes sure Spark must do authentication
    Configuration.setConfiguration(null)

    val expectedResult = Set("hello").map(Row(_))
    val query = "SELECT c0 FROM bar"
    val postgresDf = spark.read.format("jdbc")
      .option("url", postgresJdbcUrl)
      .option("keytab", postgresKeytabFullPath)
      .option("principal", postgresPrincipal)
      .option("query", query)
      .load()
    assert(postgresDf.collect().toSet === expectedResult)
    val mariaDf = spark.read.format("jdbc")
      .option("url", mariaJdbcUrl)
      .option("keytab", mariaKeytabFullPath)
      .option("principal", mariaPrincipal)
      .option("query", query)
      .load()
    assert(mariaDf.collect().toSet === expectedResult)
  }
}
