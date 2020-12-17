/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.capability;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * CapabilityStatusStore which takes care of reading , writing capability status and provides additional helpful methods
 */
public class CapabilityStatusStore implements CapabilityReader, CapabilityWriter {

  private static final Gson GSON = new Gson();
  private final TransactionRunner transactionRunner;

  @Inject
  CapabilityStatusStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  /**
   * Return the current status for a capability. If capability is not present, throws {@link IllegalArgumentException}
   *
   * @param capability
   * @return {@link CapabilityStatus}
   */
  public CapabilityStatus getStatus(String capability) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      Collection<Field<?>> keyField = Collections
        .singleton(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      Collection<String> returnField = Collections.singleton(StoreDefinition.CapabilitiesStore.STATUS_FIELD);
      Optional<StructuredRow> result = capabilityTable.read(keyField, returnField);
      return result.map(structuredRow -> CapabilityStatus
        .valueOf(structuredRow.getString(StoreDefinition.CapabilitiesStore.STATUS_FIELD).toUpperCase())).orElse(null);
    }, IOException.class);
  }

  @Override
  public boolean isEnabled(String capability) throws IOException {
    return getStatus(capability) == CapabilityStatus.ENABLED;
  }

  @Override
  public CapabilityConfig getConfig(String capability) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      Collection<Field<?>> keyField = Collections
        .singleton(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      Collection<String> returnField = Collections.singleton(StoreDefinition.CapabilitiesStore.CONFIG_FIELD);
      Optional<StructuredRow> result = capabilityTable.read(keyField, returnField);
      return result.map(structuredRow -> GSON
        .fromJson(structuredRow.getString(StoreDefinition.CapabilitiesStore.CONFIG_FIELD), CapabilityConfig.class))
        .orElse(null);
    }, IOException.class);
  }

  @Override
  public List<CapabilityStatusRecord> getAllCapabilities() throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      CloseableIterator<StructuredRow> resultIterator = capabilityTable.scan(Range.all(), Integer.MAX_VALUE);
      List<CapabilityStatusRecord> recordList = new ArrayList<>();
      while (resultIterator.hasNext()) {
        StructuredRow nextRow = resultIterator.next();
        CapabilityStatusRecord capabilityStatusRecord = new CapabilityStatusRecord(
          nextRow.getString(StoreDefinition.CapabilitiesStore.NAME_FIELD),
          CapabilityStatus.valueOf(nextRow.getString(StoreDefinition.CapabilitiesStore.STATUS_FIELD).toUpperCase()),
          GSON.fromJson(nextRow.getString(StoreDefinition.CapabilitiesStore.CONFIG_FIELD), CapabilityConfig.class));
        recordList.add(capabilityStatusRecord);
      }
      return recordList;
    }, IOException.class);
  }

  @Override
  public List<CapabilityOperationRecord> getCapabilityOperations() throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITY_OPERATIONS);
      CloseableIterator<StructuredRow> resultIterator = capabilityTable.scan(Range.all(), Integer.MAX_VALUE);
      List<CapabilityOperationRecord> recordList = new ArrayList<>();
      while (resultIterator.hasNext()) {
        StructuredRow nextRow = resultIterator.next();
        CapabilityOperationRecord capabilityOperationRecord = new CapabilityOperationRecord(
          nextRow.getString(StoreDefinition.CapabilitiesStore.NAME_FIELD),
          CapabilityAction
            .valueOf(nextRow.getString(StoreDefinition.CapabilitiesStore.ACTION_FIELD).toUpperCase()),
          GSON.fromJson(nextRow.getString(StoreDefinition.CapabilitiesStore.CONFIG_FIELD), CapabilityConfig.class));
        recordList.add(capabilityOperationRecord);
      }
      return recordList;
    }, IOException.class);
  }

  /**
   * Add or update capability
   *
   * @param capability
   * @param status
   * @throws IOException
   */
  @Override
  public void addOrUpdateCapability(String capability, CapabilityStatus status,
                                    CapabilityConfig config) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.STATUS_FIELD, status.name().toLowerCase()));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.CONFIG_FIELD, GSON.toJson(config)));
      fields.add(Fields.longField(StoreDefinition.CapabilitiesStore.UPDATED_TIME_FIELD, System.currentTimeMillis()));
      capabilityTable.upsert(fields);
    }, IOException.class);
  }

  /**
   * Delete capability
   *
   * @param capability
   * @throws IOException
   */
  @Override
  public void deleteCapability(String capability) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITIES);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      capabilityTable.delete(fields);
    }, IOException.class);
  }

  @Override
  public void addOrUpdateCapabilityOperation(String capability, CapabilityAction actionType,
                                             CapabilityConfig config) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITY_OPERATIONS);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.ACTION_FIELD, actionType.name().toLowerCase()));
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.CONFIG_FIELD, GSON.toJson(config)));
      capabilityTable.upsert(fields);
    }, IOException.class);
  }

  @Override
  public void deleteCapabilityOperation(String capability) throws IOException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable capabilityTable = context.getTable(StoreDefinition.CapabilitiesStore.CAPABILITY_OPERATIONS);
      Collection<Field<?>> fields = new ArrayList<>();
      fields.add(Fields.stringField(StoreDefinition.CapabilitiesStore.NAME_FIELD, capability));
      capabilityTable.delete(fields);
    }, IOException.class);
  }
}
