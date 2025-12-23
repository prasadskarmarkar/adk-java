/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.adk.sessions.db.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Entity for storing ADK internal metadata in the database.
 *
 * <p>This table is used to store internal information like the schema version. The
 * DatabaseSessionService will populate and utilize this table to manage database compatibility and
 * migrations.
 *
 * <p>This entity is mapped to the "adk_internal_metadata" table.
 */
@Entity
@Table(name = "adk_internal_metadata")
public class StorageMetadata {

  @Id
  @Column(name = "key", length = 128)
  private String key;

  @Column(name = "value", length = 256)
  private String value;

  public StorageMetadata() {}

  public StorageMetadata(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
