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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** A table or materialized view in the schema metadata. */
public interface RelationMetadata extends Describable {

  CqlIdentifier getKeyspace();

  CqlIdentifier getName();

  /** The unique id generated by the server for this element. */
  UUID getId();

  /**
   * Convenience method to get all the primary key columns (partition key + clustering columns) in a
   * single call.
   *
   * <p>Note that this creates a new list instance on each call.
   *
   * @see #getPartitionKey()
   * @see #getClusteringColumns()
   */
  default List<ColumnMetadata> getPrimaryKey() {
    return ImmutableList.<ColumnMetadata>builder()
        .addAll(getPartitionKey())
        .addAll(getClusteringColumns().keySet())
        .build();
  }

  List<ColumnMetadata> getPartitionKey();

  Map<ColumnMetadata, ClusteringOrder> getClusteringColumns();

  Map<CqlIdentifier, ColumnMetadata> getColumns();

  default ColumnMetadata getColumn(CqlIdentifier columnId) {
    return getColumns().get(columnId);
  }

  /**
   * Shortcut for {@link #getColumn(CqlIdentifier) getColumn(CqlIdentifier.fromCql(columnName))}.
   */
  default ColumnMetadata getColumn(String columnName) {
    return getColumn(CqlIdentifier.fromCql(columnName));
  }

  /**
   * The options of this table or materialized view.
   *
   * <p>This corresponds to the {@code WITH} clauses in the {@code CREATE} statement that would
   * recreate this element. The exact set of keys and the types of the values depend on the server
   * version that this metadata was extracted from. For example, in Cassandra 2.2 and below, {@code
   * WITH caching} takes a string argument, whereas starting with Cassandra 3.0 it is a map.
   */
  Map<CqlIdentifier, Object> getOptions();
}