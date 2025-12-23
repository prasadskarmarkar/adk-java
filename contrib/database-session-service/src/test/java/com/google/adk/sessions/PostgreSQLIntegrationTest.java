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
package com.google.adk.sessions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.adk.testing.TestDatabaseConfig;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for DatabaseSessionService with real PostgreSQL 16 database.
 *
 * <p>This test suite verifies that DatabaseSessionService works correctly with PostgreSQL,
 * including: - Schema creation via Flyway migrations - Event actions storage with JSONB column type
 * - Session CRUD operations - Event filtering and pagination
 *
 * <p>Prerequisites: Start PostgreSQL test database with:
 *
 * <pre>{@code
 * docker-compose -f scripts/docker-compose.test.yml up -d postgres-test
 * }</pre>
 *
 * <p>Configuration: - Host: localhost:5433 - Database: adk_test - User: adk_user - Password:
 * adk_password
 */
@Tag("integration")
public class PostgreSQLIntegrationTest {

  private static final String TEST_DB_URL = TestDatabaseConfig.POSTGRES_JDBC_URL;
  private String TEST_APP_NAME;
  private String TEST_USER_ID;

  private DatabaseSessionService sessionService;

  @BeforeEach
  public void setUp() {
    assumeTrue(
        TestDatabaseConfig.isPostgreSQLAvailable(),
        TestDatabaseConfig.getDatabaseNotAvailableMessage("PostgreSQL"));

    TEST_APP_NAME = "postgresql-test-app-" + System.currentTimeMillis();
    TEST_USER_ID = "postgresql-test-user-" + System.currentTimeMillis();

    Map<String, Object> properties =
        Map.of(
            "hibernate.show_sql", "true",
            "hibernate.format_sql", "true");

    sessionService = new DatabaseSessionService(TEST_DB_URL, properties);
  }

  @AfterEach
  public void tearDown() {
    if (sessionService != null) {
      sessionService.close();
    }
  }

  @Test
  public void testEventActionsWithStateDeltaAndEscalate() {
    String sessionId = "actions-test-session-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    ConcurrentHashMap<String, Object> stateDelta = new ConcurrentHashMap<>();
    stateDelta.put("weather_data", "sunny");
    stateDelta.put("temperature", 72);

    EventActions actions = EventActions.builder().stateDelta(stateDelta).escalate(true).build();

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-agent")
            .content(Content.fromParts(Part.fromText("Processing weather data")))
            .actions(actions)
            .timestamp(Instant.now().toEpochMilli())
            .build();

    Session updatedSession =
        sessionService.appendEvent(TEST_APP_NAME, TEST_USER_ID, sessionId, event).blockingGet();

    assertNotNull(updatedSession);
    assertEquals(1, updatedSession.events().size());

    Event retrievedEvent = updatedSession.events().get(0);
    assertNotNull(retrievedEvent.actions());
    assertEquals(2, retrievedEvent.actions().stateDelta().size());
    assertEquals("sunny", retrievedEvent.actions().stateDelta().get("weather_data"));
    assertEquals(72, retrievedEvent.actions().stateDelta().get("temperature"));
    assertEquals(true, retrievedEvent.actions().escalate().get());
  }

  @Test
  public void testEventActionsWithMultipleFields() {
    String sessionId = "multiple-actions-test-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    ConcurrentHashMap<String, Object> stateDelta = new ConcurrentHashMap<>();
    stateDelta.put("key1", "value1");
    stateDelta.put("key2", 42);

    ConcurrentHashMap<String, Part> artifactDelta = new ConcurrentHashMap<>();
    artifactDelta.put("artifact1", Part.fromText("artifact content"));

    EventActions actions =
        EventActions.builder()
            .stateDelta(stateDelta)
            .artifactDelta(artifactDelta)
            .transferToAgent("agent-2")
            .skipSummarization(true)
            .build();

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-agent")
            .content(Content.fromParts(Part.fromText("Complex action event")))
            .actions(actions)
            .timestamp(Instant.now().toEpochMilli())
            .build();

    Session updatedSession =
        sessionService.appendEvent(TEST_APP_NAME, TEST_USER_ID, sessionId, event).blockingGet();

    assertNotNull(updatedSession);
    Event retrievedEvent = updatedSession.events().get(0);
    assertNotNull(retrievedEvent.actions());
    assertEquals(2, retrievedEvent.actions().stateDelta().size());
    assertEquals(1, retrievedEvent.actions().artifactDelta().size());
    assertEquals("agent-2", retrievedEvent.actions().transferToAgent().get());
    assertEquals(true, retrievedEvent.actions().skipSummarization().get());
  }

  @Test
  public void testLifecycleNoSession() {
    assertNull(
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "non-existent-session", Optional.empty())
            .blockingGet());

    ListSessionsResponse sessionsResponse =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertTrue(sessionsResponse.sessions().isEmpty());

    ListEventsResponse eventsResponse =
        sessionService
            .listEvents(TEST_APP_NAME, TEST_USER_ID, "non-existent-session")
            .blockingGet();
    assertTrue(eventsResponse.events().isEmpty());
  }

  @Test
  public void testLifecycleCreateSession() {
    String sessionId = "create-test-" + System.currentTimeMillis();
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
            .blockingGet();

    assertNotNull(session.id());
    assertEquals(TEST_APP_NAME, session.appName());
    assertEquals(TEST_USER_ID, session.userId());
    assertTrue(session.state().isEmpty());
  }

  @Test
  public void testLifecycleGetSession() {
    String sessionId = "get-test-" + System.currentTimeMillis();
    Session created =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
            .blockingGet();

    Session retrieved =
        sessionService
            .getSession(created.appName(), created.userId(), created.id(), Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals(created.id(), retrieved.id());
  }

  @Test
  public void testSessionCRUDOperations() {
    String sessionId = "crud-test-" + System.currentTimeMillis();
    ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();
    initialState.put("status", "active");

    Session created =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, initialState, sessionId)
            .blockingGet();
    assertNotNull(created);
    assertEquals(sessionId, created.id());
    assertEquals("active", created.state().get("status"));

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    assertNotNull(retrieved);
    assertEquals(sessionId, retrieved.id());

    sessionService.deleteSession(TEST_APP_NAME, TEST_USER_ID, sessionId).blockingAwait();
    assertNull(
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet());
  }

  @Test
  public void testListSessionsEmpty() {
    String uniqueUserId = "empty-user-" + System.currentTimeMillis();
    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, uniqueUserId).blockingGet();

    assertNotNull(response);
    assertEquals(0, response.sessions().size());
  }

  @Test
  public void testListSessions() {
    String uniqueUserId = "list-user-" + System.currentTimeMillis();
    String sessionId1 = "list-test-1-" + System.currentTimeMillis();
    String sessionId2 = "list-test-2-" + System.currentTimeMillis();

    sessionService
        .createSession(TEST_APP_NAME, uniqueUserId, new ConcurrentHashMap<>(), sessionId1)
        .blockingGet();
    sessionService
        .createSession(TEST_APP_NAME, uniqueUserId, new ConcurrentHashMap<>(), sessionId2)
        .blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, uniqueUserId).blockingGet();

    assertNotNull(response);
    List<Session> sessions = response.sessions();
    assertEquals(2, sessions.size());
    assertTrue(sessions.stream().anyMatch(s -> s.id().equals(sessionId1)));
    assertTrue(sessions.stream().anyMatch(s -> s.id().equals(sessionId2)));
  }

  @Test
  public void testLifecycleDeleteSession() {
    String sessionId = "delete-test-" + System.currentTimeMillis();
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
            .blockingGet();

    sessionService.deleteSession(session.appName(), session.userId(), session.id()).blockingAwait();

    assertNull(
        sessionService
            .getSession(session.appName(), session.userId(), session.id(), Optional.empty())
            .blockingGet());
  }

  @Test
  public void testAppendEventToNonExistentSession() {
    String nonExistentId = "non-existent-" + System.currentTimeMillis();
    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-author")
            .content(Content.fromParts(Part.fromText("Test event")))
            .timestamp(Instant.now().toEpochMilli())
            .build();

    assertThrows(
        SessionNotFoundException.class,
        () ->
            sessionService
                .appendEvent(TEST_APP_NAME, TEST_USER_ID, nonExistentId, event)
                .blockingGet());
  }

  @Test
  public void testEventContentStorage() {
    String sessionId = "content-test-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-author")
            .content(Content.fromParts(Part.fromText("Hello, PostgreSQL Integration Test!")))
            .timestamp(Instant.now().toEpochMilli())
            .build();

    Session updatedSession =
        sessionService.appendEvent(TEST_APP_NAME, TEST_USER_ID, sessionId, event).blockingGet();

    assertNotNull(updatedSession);
    assertEquals(1, updatedSession.events().size());
    Event retrievedEvent = updatedSession.events().get(0);
    assertEquals(event.id(), retrievedEvent.id());
    assertEquals(event.author(), retrievedEvent.author());
    assertEquals(
        "Hello, PostgreSQL Integration Test!",
        retrievedEvent.content().flatMap(c -> c.parts()).stream()
            .flatMap(List::stream)
            .flatMap(p -> p.text().stream())
            .findFirst()
            .orElse(""));
  }

  @Test
  public void testJSONBDataTypeForActions() {
    String sessionId = "jsonb-test-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    ConcurrentHashMap<String, Object> complexState = new ConcurrentHashMap<>();
    complexState.put("nested", Map.of("key1", "value1", "key2", List.of(1, 2, 3)));
    complexState.put("array", List.of("item1", "item2", "item3"));

    EventActions actions =
        EventActions.builder().stateDelta(complexState).endInvocation(true).build();

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-agent")
            .content(Content.fromParts(Part.fromText("Testing JSONB storage")))
            .actions(actions)
            .timestamp(Instant.now().toEpochMilli())
            .build();

    Session updatedSession =
        sessionService.appendEvent(TEST_APP_NAME, TEST_USER_ID, sessionId, event).blockingGet();

    assertNotNull(updatedSession);
    Event retrievedEvent = updatedSession.events().get(0);
    assertNotNull(retrievedEvent.actions());
    assertEquals(2, retrievedEvent.actions().stateDelta().size());
    assertEquals(true, retrievedEvent.actions().endInvocation().get());
  }

  @Test
  public void testGetSession_EmptyOptionalConfig() {
    String sessionId = createSessionWithEvents(10);

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(session);
    assertEquals(10, session.events().size());
  }

  @Test
  public void testGetSession_NumRecentEvents_With10Events() {
    String sessionId = createSessionWithEvents(10);

    GetSessionConfig config = GetSessionConfig.builder().numRecentEvents(5).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(5, session.events().size());
    assertEquals("event-6", session.events().get(0).id());
    assertEquals("event-10", session.events().get(4).id());
  }

  @Test
  public void testGetSession_NumRecentEvents_With20Events() {
    String sessionId = createSessionWithEvents(20);

    GetSessionConfig config = GetSessionConfig.builder().numRecentEvents(3).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(3, session.events().size());
    assertEquals("event-18", session.events().get(0).id());
    assertEquals("event-19", session.events().get(1).id());
    assertEquals("event-20", session.events().get(2).id());
  }

  @Test
  public void testGetSession_NumRecentEvents_With100Events() {
    String sessionId = createSessionWithEvents(100);

    GetSessionConfig config = GetSessionConfig.builder().numRecentEvents(10).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(10, session.events().size());
    assertEquals("event-91", session.events().get(0).id());
    assertEquals("event-100", session.events().get(9).id());
  }

  @Test
  public void testGetSession_AfterTimestamp_With10Events() {
    String sessionId = createSessionWithEvents(10);

    Session allEvents =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    Instant event5Timestamp = Instant.ofEpochMilli(allEvents.events().get(4).timestamp());

    GetSessionConfig config = GetSessionConfig.builder().afterTimestamp(event5Timestamp).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(5, session.events().size());
    assertEquals("event-6", session.events().get(0).id());
    assertEquals("event-10", session.events().get(4).id());
  }

  @Test
  public void testGetSession_AfterTimestamp_With20Events() {
    String sessionId = createSessionWithEvents(20);

    Session allEvents =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    Instant event15Timestamp = Instant.ofEpochMilli(allEvents.events().get(14).timestamp());

    GetSessionConfig config = GetSessionConfig.builder().afterTimestamp(event15Timestamp).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(5, session.events().size());
    assertEquals("event-16", session.events().get(0).id());
    assertEquals("event-20", session.events().get(4).id());
  }

  @Test
  public void testGetSession_AfterTimestamp_With100Events() {
    String sessionId = createSessionWithEvents(100);

    Session allEvents =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    Instant event90Timestamp = Instant.ofEpochMilli(allEvents.events().get(89).timestamp());

    GetSessionConfig config = GetSessionConfig.builder().afterTimestamp(event90Timestamp).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(10, session.events().size());
    assertEquals("event-91", session.events().get(0).id());
    assertEquals("event-100", session.events().get(9).id());
  }

  @Test
  public void testGetSession_NumRecentEvents_TakesPrecedenceOverTimestamp() {
    String sessionId = createSessionWithEvents(20);

    Instant threshold = Instant.now().plusSeconds(10);
    GetSessionConfig config =
        GetSessionConfig.builder().numRecentEvents(3).afterTimestamp(threshold).build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(3, session.events().size());
    assertEquals("event-18", session.events().get(0).id());
    assertEquals("event-19", session.events().get(1).id());
    assertEquals("event-20", session.events().get(2).id());
  }

  @Test
  public void testGetSession_NumRecentEvents_RequestMoreThanAvailable() {
    String sessionId = createSessionWithEvents(10);

    GetSessionConfig config = GetSessionConfig.builder().numRecentEvents(20).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(10, session.events().size());
  }

  @Test
  public void testGetSession_ChronologicalOrder_With20Events() {
    String sessionId = createSessionWithEvents(20);

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(session);
    assertEquals(20, session.events().size());

    for (int i = 0; i < 20; i++) {
      assertEquals("event-" + (i + 1), session.events().get(i).id());
      if (i > 0) {
        long prevTimestamp = session.events().get(i - 1).timestamp();
        long currTimestamp = session.events().get(i).timestamp();
        assertTrue(currTimestamp >= prevTimestamp, "Events should be in chronological order");
      }
    }
  }

  @Test
  public void testAppendEventUpdatesSessionState() {
    String sessionId = "state-test-" + System.currentTimeMillis();
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
            .blockingGet();

    ConcurrentHashMap<String, Object> stateDelta = new ConcurrentHashMap<>();
    stateDelta.put("sessionKey", "sessionValue");
    stateDelta.put("_app_appKey", "appValue");
    stateDelta.put("_user_userKey", "userValue");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-author")
            .content(Content.fromParts(Part.fromText("Test event")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(stateDelta).build())
            .build();

    sessionService.appendEvent(session, event).blockingGet();

    Session retrievedSession =
        sessionService
            .getSession(session.appName(), session.userId(), session.id(), Optional.empty())
            .blockingGet();
    assertNotNull(retrievedSession);
    assertEquals("sessionValue", retrievedSession.state().get("sessionKey"));
    assertEquals("appValue", retrievedSession.state().get("_app_appKey"));
    assertEquals("userValue", retrievedSession.state().get("_user_userKey"));
  }

  @Test
  public void testMigrationWithDefaultBaselineOnMigrateFalse_CleanDatabase() {
    try {
      Flyway flyway =
          Flyway.configure()
              .dataSource(TEST_DB_URL, null, null)
              .locations("classpath:db/migration/postgresql")
              .cleanDisabled(false)
              .load();
      flyway.clean();

      System.clearProperty("FLYWAY_BASELINE_ON_MIGRATE");

      String testAppName = "baseline-false-app-" + System.currentTimeMillis();
      String testUserId = "baseline-false-user-" + System.currentTimeMillis();

      DatabaseSessionService testService = new DatabaseSessionService(TEST_DB_URL, Map.of());

      Session session =
          testService
              .createSession(testAppName, testUserId, new ConcurrentHashMap<>(), "test-session")
              .blockingGet();
      assertNotNull(session);

      testService.close();
    } catch (Exception e) {
      throw new RuntimeException("Test failed", e);
    }
  }

  @Test
  public void testMigrationWithBaselineOnMigrateTrue_ExistingTables() {
    try {
      Flyway flyway =
          Flyway.configure()
              .dataSource(TEST_DB_URL, null, null)
              .locations("classpath:db/migration/postgresql")
              .cleanDisabled(false)
              .load();
      flyway.clean();

      createPostgreSQLTablesManually(TEST_DB_URL);

      System.setProperty("FLYWAY_BASELINE_ON_MIGRATE", "true");

      try {
        String testAppName = "baseline-true-app-" + System.currentTimeMillis();
        String testUserId = "baseline-true-user-" + System.currentTimeMillis();

        DatabaseSessionService testService = new DatabaseSessionService(TEST_DB_URL, Map.of());

        Session session =
            testService
                .createSession(
                    testAppName, testUserId, new ConcurrentHashMap<>(), "baseline-session")
                .blockingGet();
        assertNotNull(session);

        testService.close();
      } finally {
        System.clearProperty("FLYWAY_BASELINE_ON_MIGRATE");
      }

      flyway.clean();
    } catch (Exception e) {
      throw new RuntimeException("Test failed", e);
    }
  }

  @Test
  public void testSchemaValidationAfterMigration() {
    try {
      Flyway flyway =
          Flyway.configure()
              .dataSource(TEST_DB_URL, null, null)
              .locations("classpath:db/migration/postgresql")
              .cleanDisabled(false)
              .load();
      flyway.clean();

      DatabaseSessionService testService = new DatabaseSessionService(TEST_DB_URL, Map.of());

      Flyway validationFlyway =
          Flyway.configure()
              .dataSource(TEST_DB_URL, null, null)
              .locations("classpath:db/migration/postgresql")
              .load();
      validationFlyway.validate();

      testService.close();
    } catch (Exception e) {
      throw new RuntimeException("Schema validation failed", e);
    }
  }

  private void createPostgreSQLTablesManually(String dbUrl) throws Exception {
    try (Connection conn = DriverManager.getConnection(dbUrl, null, null);
        Statement stmt = conn.createStatement()) {

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS sessions ("
              + "app_name VARCHAR(128) NOT NULL, "
              + "user_id VARCHAR(128) NOT NULL, "
              + "id VARCHAR(128) NOT NULL, "
              + "state JSONB, "
              + "create_time TIMESTAMP, "
              + "update_time TIMESTAMP, "
              + "PRIMARY KEY (app_name, user_id, id))");

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS app_states ("
              + "app_name VARCHAR(128) NOT NULL, "
              + "state JSONB, "
              + "update_time TIMESTAMP, "
              + "PRIMARY KEY (app_name))");

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS user_states ("
              + "app_name VARCHAR(128) NOT NULL, "
              + "user_id VARCHAR(128) NOT NULL, "
              + "state JSONB, "
              + "update_time TIMESTAMP, "
              + "PRIMARY KEY (app_name, user_id))");
    }
  }

  private String createSessionWithEvents(int numEvents) {
    String sessionId = "config-test-" + System.currentTimeMillis() + "-" + UUID.randomUUID();
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
            .blockingGet();

    Instant baseTime = Instant.now();

    for (int i = 1; i <= numEvents; i++) {
      Event event =
          Event.builder()
              .id("event-" + i)
              .author("test-author")
              .content(Content.fromParts(Part.fromText("Event content " + i)))
              .timestamp(baseTime.plusSeconds(i).toEpochMilli())
              .build();

      sessionService.appendEvent(TEST_APP_NAME, TEST_USER_ID, session.id(), event).blockingGet();

      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    return session.id();
  }
}
