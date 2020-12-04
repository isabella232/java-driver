/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.examples.failover;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import reactor.core.publisher.Flux;

/**
 * This example illustrates how to implement a cross-datacenter failover strategy from application
 * code.
 *
 * <p>Starting with driver 4.10, cross-datacenter failover is also provided as a configuration
 * option for built-in load balancing policies. See the <a
 * href="https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/">manual</a>.
 *
 * <p>This example demonstrates how to achieve the same effect in application code, which confers
 * more fained-grained control over which statements should be retried and where.
 *
 * <p>This examples showcases cross-datacenter failover with 3 different programming styles:
 * synchronous, asynchronous and reactive (using the Reactor library). The 3 styles are identical in
 * terms of failover effect; they are all included merely to help programmers pick the variant that
 * is closest to the style they use.
 *
 * <p>Preconditions:
 *
 * <ul>
 *   <li>An Apache Cassandra(R) cluster with two datacenters, dc1 and dc2, containing at least 3
 *       nodes in each datacenter, is running and accessible through the contact point:
 *       127.0.0.1:9042.
 * </ul>
 *
 * <p>Side effects:
 *
 * <ol>
 *   <li>Creates a new keyspace {@code failover} in the cluster, with replication factor 3 in both
 *       datacenters. If a keyspace with this name already exists, it will be reused;
 *   <li>Creates a new table {@code failover.orders}. If a table with that name exists already, it
 *       will be reused;
 *   <li>Tries to write a row in the table using the local datacenter dc1;
 *   <li>If the local datacenter dc1 is down, retries the write in the remote datacenter dc2.
 * </ol>
 *
 * @see <a href="https://docs.datastax.com/en/developer/java-driver/latest">Java driver online
 *     manual</a>
 */
public class CrossDatacenterFailover {

  public static void main(String[] args) throws Exception {

    CrossDatacenterFailover client = new CrossDatacenterFailover();

    try {

      // Note: when this example is executed, at least the local DC must be available
      // since the driver will try to reach contact points in that DC.

      client.connect();
      client.createSchema();

      // To fully exercise this example, try to stop the entire dc1 here; then observe how
      // the writes executed below will first fail in dc1, then be diverted to dc2, where they will
      // succeed.

      client.writeSync();
      client.writeAsync();
      client.writeReactive();

    } finally {
      client.close();
    }
  }

  private CqlSession session;

  private CrossDatacenterFailover() {}

  /** Initiates a connection to the cluster. */
  private void connect() {

    // For simplicity, this example uses a 100% in-memory configuration loader, but the same
    // configuration can be achieved with the more traditional file-based approach.
    // Simply put the below snippet in your application.conf file to get the same config:

    /*
    datastax-java-driver {
      basic.contact-points = [ "127.0.0.1:9042" ]
      basic.load-balancing-policy.local-datacenter = "dc1"
      basic.request.consistency = QUORUM
      profiles {
        remote {
          basic.load-balancing-policy.local-datacenter = "dc2"
          basic.request.consistency = ONE
        }
      }
    }
    */

    OptionsMap options = OptionsMap.driverDefaults();
    // set the datacenter to dc1 in the default profile; this makes dc1 the local datacenter
    options.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1");
    // set the datacenter to dc2 in the "remote" profile
    options.put("remote", TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc2");
    // make sure to provide a contact point belonging to dc1, not dc2!
    options.put(TypedDriverOption.CONTACT_POINTS, Collections.singletonList("127.0.0.1:9042"));
    // in this example, the default consistency level is QUORUM
    options.put(TypedDriverOption.REQUEST_CONSISTENCY, "QUORUM");
    // but when failing over, the consistency level will be automatically downgraded to ONE
    options.put("remote", TypedDriverOption.REQUEST_CONSISTENCY, "ONE");

    session = CqlSession.builder().withConfigLoader(DriverConfigLoader.fromMap(options)).build();

    System.out.println("Connected to cluster with session: " + session.getName());
  }

  /** Creates the schema (keyspace) and table for this example. */
  private void createSchema() {

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS failover WITH replication "
            + "= {'class':'NetworkTopologyStrategy', 'dc1':3, 'dc2':3}");

    session.execute(
        "CREATE TABLE IF NOT EXISTS failover.orders ("
            + "product_id uuid,"
            + "timestamp timestamp,"
            + "price double,"
            + "PRIMARY KEY (product_id,timestamp)"
            + ")");
  }

  /** Inserts data synchronously using the local DC, retrying if necessary in a remote DC. */
  private void writeSync() {

    System.out.println("------- DC failover (sync) ------- ");

    Statement<?> statement =
        SimpleStatement.newInstance(
            "INSERT INTO failover.orders "
                + "(product_id, timestamp, price) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26T13:53:46.345+01:00',"
                + "2.34)");

    try {

      // try the statement using the default profile, which targets the local datacenter dc1.
      session.execute(statement);

      System.out.println("Write succeeded");

    } catch (DriverException e) {

      if (shouldFailover(e)) {

        System.out.println("Write failed in local DC, retrying in remote DC");

        try {

          // try the statement using the remote profile, which targets the remote datacenter dc2.
          session.execute(statement.setExecutionProfileName("remote"));

          System.out.println("Write succeeded");

        } catch (DriverException e2) {

          System.out.println("Write failed in remote DC");

          e2.printStackTrace();
        }
      }
    }
    // let other errors propagate
  }

  /** Inserts data asynchronously using the local DC, retrying if necessary in a remote DC. */
  private void writeAsync() throws ExecutionException, InterruptedException {

    System.out.println("------- DC failover (async) ------- ");

    Statement<?> statement =
        SimpleStatement.newInstance(
            "INSERT INTO failover.orders "
                + "(product_id, timestamp, price) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26T13:53:46.345+01:00',"
                + "2.34)");

    CompletionStage<AsyncResultSet> result =
        // try the statement using the default profile, which targets the local datacenter dc1.
        session
            .executeAsync(statement)
            .handle(
                (rs, error) -> {
                  if (error == null) {
                    return CompletableFuture.completedFuture(rs);
                  } else {
                    if (error instanceof DriverException
                        && shouldFailover((DriverException) error)) {
                      System.out.println("Write failed in local DC, retrying in remote DC");
                      // try the statement using the remote profile, which targets the remote
                      // datacenter dc2.
                      return session.executeAsync(statement.setExecutionProfileName("remote"));
                    }
                    // let other errors propagate
                    return CompletableFutures.<AsyncResultSet>failedFuture(error);
                  }
                })
            // unwrap (flatmap) the nested future
            .thenCompose(future -> future)
            .whenComplete(
                (rs, error) -> {
                  if (error == null) {
                    System.out.println("Write succeeded");
                  } else {
                    System.out.println("Write failed in remote DC");
                    error.printStackTrace();
                  }
                });

    // for the sake of this example, wait for the operation to finish
    result.toCompletableFuture().get();
  }

  /** Inserts data reactively using the local DC, retrying if necessary in a remote DC. */
  private void writeReactive() {

    System.out.println("------- DC failover (reactive) ------- ");

    Statement<?> statement =
        SimpleStatement.newInstance(
            "INSERT INTO failover.orders "
                + "(product_id, timestamp, price) "
                + "VALUES ("
                + "756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'2018-02-26T13:53:46.345+01:00',"
                + "2.34)");

    Flux<ReactiveRow> result =
        // try the statement using the default profile, which targets the local datacenter dc1.
        Flux.from(session.executeReactive(statement))
            .onErrorResume(
                DriverException.class,
                error -> {
                  if (shouldFailover(error)) {
                    System.out.println("Write failed in local DC, retrying in remote DC");
                    // try the statement using the remote profile, which targets the remote
                    // datacenter dc2.
                    return session.executeReactive(statement.setExecutionProfileName("remote"));
                  } else {
                    return Flux.error(error);
                  }
                })
            .doOnComplete(() -> System.out.println("Write succeeded"))
            .doOnError(
                error -> {
                  System.out.println("Write failed");
                  error.printStackTrace();
                });

    // for the sake of this example, wait for the operation to finish
    result.blockLast();
  }

  /**
   * Analyzes the error and decides whether to failover to a remote DC. Adjust this logic to your
   * needs.
   *
   * <p>In a typical whole datacenter outage scenario, the error you get is {@link
   * NoNodeAvailableException}.
   *
   * <p>In case of a partial datacenter outage (that is, some nodes were up in the datacenter, but
   * none could handle the request):
   *
   * <ol>
   *   <li>If only one coordinator was tried: you usually get back a {@link
   *       QueryExecutionException}. Is is generally not worth failing over since it means the
   *       driver itself thinks that the failure is permanent.
   *   <li>If more than one coordinator was tried: the error you get is {@link
   *       AllNodesFailedException}, and you can inspect {@link
   *       AllNodesFailedException#getAllErrors()} to understand what happened with each
   *       coordinator.
   * </ol>
   */
  private boolean shouldFailover(DriverException e) {
    if (e instanceof QueryExecutionException) {
      // This includes: UnavailableException, WriteTimeoutException, ReadTimeoutException, etc.
      // It is usually not worth failing over to a remote DC here, as the request is likely
      // to fail again, *unless* the consistency level is DC-local, or if you plan to downgrade the
      // consistency level when retrying.
      Node coordinator = Objects.requireNonNull(e.getExecutionInfo().getCoordinator());
      System.out.printf(
          "Node %s in DC %s failed: %s%n",
          coordinator.getEndPoint(), coordinator.getDatacenter(), e);
      return false;
    } else if (e instanceof AllNodesFailedException) {
      if (e instanceof NoNodeAvailableException) {
        // This could be a total DC outage.
        System.out.println("All nodes were down in this datacenter");
      } else {
        // This could be a partial DC outage.
        // In a real application, here you would inspect how many coordinators were tried,
        // and which errors they returned.
        ((AllNodesFailedException) e)
            .getAllErrors()
            .forEach(
                (coordinator, errors) -> {
                  System.out.printf(
                      "Node %s in DC %s was tried %d times but failed with:%n",
                      coordinator.getEndPoint(), coordinator.getDatacenter(), errors.size());
                  for (Throwable error : errors) {
                    System.out.printf("\t- %s%n", error);
                  }
                });
      }
      // If no nodes in the local DC could handle the request, assume this was a complete or partial
      // DC outage and fail over to a remote DC.
      return true;
    } else {
      // Other errors: don't failover.
      System.out.println("Unexpected error: " + e);
      return false;
    }
  }

  private void close() {
    if (session != null) {
      session.close();
    }
  }
}
