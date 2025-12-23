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

import com.google.adk.events.Event;
import com.google.adk.sessions.db.converter.JsonUserType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Map;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entity for storing event data in the database using v1 schema format.
 *
 * <p>This entity is mapped to the "events" table and uses a simplified JSON-based storage approach
 * that matches Python ADK v1 schema. Instead of storing individual event fields in separate
 * columns, the entire Event object is serialized to a single JSON column (event_data).
 *
 * <p><b>Design Pattern: Persistence Model vs Domain Model Separation</b>
 *
 * <p>This class serves as the <b>persistence model</b> for {@link Event} (the domain model). This
 * separation exists because:
 *
 * <ul>
 *   <li><b>JPA Requirements:</b> JPA entities must be mutable with setters, no-arg constructors,
 *       and cannot have final fields - incompatible with immutable domain models
 *   <li><b>Schema Evolution:</b> Database schema (v1 format with JSON) is decoupled from domain
 *       model changes
 *   <li><b>Composite Keys:</b> Database requires {@code appName}, {@code userId}, {@code sessionId}
 *       for foreign key relationships, which don't belong in the domain Event
 *   <li><b>Python Compatibility:</b> Matches Python ADK v1 schema for cross-language
 *       interoperability
 * </ul>
 *
 * <p><b>Mapper Contract:</b>
 *
 * <ul>
 *   <li>{@link #fromDomainEvent(Event, StorageSession)} - Converts domain Event to persistence
 *       StorageEvent
 *   <li>{@link #toDomainEvent()} - Reconstructs domain Event from persistence StorageEvent
 * </ul>
 *
 * <p><b>Important:</b> All fields in {@link Event} MUST be serializable to JSON to prevent data
 * loss. The event_data column stores the complete Event as JSON.
 */
@Entity
@Table(name = "events")
@IdClass(EventId.class)
public class StorageEvent {

  private static final Logger logger = LoggerFactory.getLogger(StorageEvent.class);

  @Id
  @Column(name = "id", length = 128)
  private String id;

  @Id
  @Column(name = "app_name", length = 128)
  private String appName;

  @Id
  @Column(name = "user_id", length = 128)
  private String userId;

  @Id
  @Column(name = "session_id", length = 128)
  private String sessionId;

  @Column(name = "invocation_id", length = 256)
  private String invocationId;

  @Column(name = "timestamp")
  private Instant timestamp;

  @Column(name = "event_data")
  @Type(JsonUserType.class)
  private Map<String, Object> eventData;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumns({
    @JoinColumn(
        name = "app_name",
        referencedColumnName = "app_name",
        insertable = false,
        updatable = false),
    @JoinColumn(
        name = "user_id",
        referencedColumnName = "user_id",
        insertable = false,
        updatable = false),
    @JoinColumn(
        name = "session_id",
        referencedColumnName = "id",
        insertable = false,
        updatable = false)
  })
  private StorageSession session;

  public StorageEvent() {}

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getInvocationId() {
    return invocationId;
  }

  public void setInvocationId(String invocationId) {
    this.invocationId = invocationId;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }

  public Map<String, Object> getEventData() {
    return eventData;
  }

  public void setEventData(Map<String, Object> eventData) {
    this.eventData = eventData;
  }

  public StorageSession getSession() {
    return session;
  }

  public void setSession(StorageSession session) {
    this.session = session;
  }

  /**
   * Converts this storage entity to a domain model Event.
   *
   * @return A domain Event object reconstructed from the JSON event_data
   */
  public Event toDomainEvent() {
    try {
      String json = com.google.adk.JsonBaseModel.getMapper().writeValueAsString(this.eventData);
      Event event = com.google.adk.JsonBaseModel.getMapper().readValue(json, Event.class);

      event.setId(this.id);
      event.setInvocationId(this.invocationId);
      event.setTimestamp(this.timestamp.toEpochMilli());

      return event;
    } catch (Exception e) {
      logger.error("Failed to deserialize event data for event {}: {}", this.id, e.getMessage(), e);
      throw new RuntimeException("Failed to convert StorageEvent to Event", e);
    }
  }

  /**
   * Creates a StorageEvent entity from a domain Event model.
   *
   * @param event The domain Event to convert
   * @param session The parent StorageSession
   * @return A StorageEvent entity with event data serialized to JSON
   */
  public static StorageEvent fromDomainEvent(Event event, StorageSession session) {
    StorageEvent storageEvent = new StorageEvent();
    storageEvent.setId(event.id());
    storageEvent.setAppName(session.getAppName());
    storageEvent.setUserId(session.getUserId());
    storageEvent.setSessionId(session.getId());
    storageEvent.setSession(session);
    storageEvent.setInvocationId(event.invocationId());
    storageEvent.setTimestamp(Instant.ofEpochMilli(event.timestamp()));

    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> eventDataMap =
          com.google.adk.JsonBaseModel.getMapper()
              .convertValue(
                  event,
                  new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});

      eventDataMap.remove("id");
      eventDataMap.remove("invocationId");
      eventDataMap.remove("timestamp");

      storageEvent.setEventData(eventDataMap);
    } catch (Exception e) {
      logger.error(
          "Failed to serialize event data for event {}: {}", event.id(), e.getMessage(), e);
      throw new RuntimeException("Failed to convert Event to StorageEvent", e);
    }

    return storageEvent;
  }
}
