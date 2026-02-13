package com.google.adk.sessions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.adk.testing.TestDatabaseConfig;
import com.google.genai.types.Content;
import com.google.genai.types.FileData;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionResponse;
import com.google.genai.types.Part;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class PostgreSQLFunctionalTest {

  private static final String TEST_DB_URL = TestDatabaseConfig.POSTGRES_JDBC_URL;
  private String TEST_APP_NAME;
  private String TEST_USER_ID;

  private DatabaseSessionService sessionService;

  @BeforeEach
  public void setUp() {
    assumeTrue(
        TestDatabaseConfig.isPostgreSQLAvailable(),
        TestDatabaseConfig.getDatabaseNotAvailableMessage("PostgreSQL"));

    TEST_APP_NAME = "jdbc-postgres-test-app-" + System.currentTimeMillis();
    TEST_USER_ID = "jdbc-postgres-test-user-" + System.currentTimeMillis();

    sessionService = new DatabaseSessionService(TEST_DB_URL);
  }

  @AfterEach
  public void tearDown() {
    if (sessionService != null) {
      sessionService.close();
    }
  }

  @Test
  public void testBasicSessionOperations() {
    String sessionId = "postgres-basic-test-" + System.currentTimeMillis();
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put("key", "value");

    Session session =
        sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, sessionId).blockingGet();

    assertNotNull(session);
    assertEquals(sessionId, session.id());
    assertEquals("value", session.state().get("key"));
  }

  @Test
  public void testEventActionsWithStateDelta() {
    String sessionId = "postgres-actions-test-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    ConcurrentHashMap<String, Object> stateDelta = new ConcurrentHashMap<>();
    stateDelta.put("count", 1);
    stateDelta.put("app:shared", "global");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-author")
            .content(Content.fromParts(Part.fromText("Test event")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(stateDelta).build())
            .build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    sessionService.appendEvent(session, event).blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals(1, retrieved.state().get("count"));
    assertEquals("global", retrieved.state().get("app:shared"));
  }

  @Test
  public void testJSONBStorageAndRetrieval() {
    String sessionId = "postgres-jsonb-test-" + System.currentTimeMillis();
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put("nested", java.util.Map.of("inner", "value"));

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, sessionId).blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertNotNull(retrieved.state().get("nested"));
  }

  @Test
  public void testUpsertAppState() {
    String sessionId1 = "postgres-upsert-1-" + System.currentTimeMillis();
    String sessionId2 = "postgres-upsert-2-" + System.currentTimeMillis();

    ConcurrentHashMap<String, Object> state1 = new ConcurrentHashMap<>();
    state1.put("app:config", "value1");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state1, sessionId1).blockingGet();

    ConcurrentHashMap<String, Object> state2 = new ConcurrentHashMap<>();
    state2.put("app:config", "value2");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state2, sessionId2).blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId1, Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals("value2", retrieved.state().get("app:config"));
  }

  @Test
  public void testGetSessionWithInvalidConfig() {
    String sessionId = "postgres-invalid-config-test-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    // Add 5 events
    for (int i = 1; i <= 5; i++) {
      Event event =
          Event.builder()
              .id("event-" + i)
              .author("test-author")
              .content(Content.fromParts(Part.fromText("Event " + i)))
              .timestamp(Instant.now().toEpochMilli())
              .build();
      Session session =
          sessionService
              .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
              .blockingGet();

      sessionService.appendEvent(session, event).blockingGet();
    }

    // Test negative numRecentEvents: -1 should be treated as abs(-1) = 1 (last 1 event)
    GetSessionConfig negativeNumEvents = GetSessionConfig.builder().numRecentEvents(-1).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(negativeNumEvents))
            .blockingGet();

    assertNotNull(session);
    // Should return exactly 1 event (the most recent one)
    assertEquals(
        1,
        session.events().size(),
        "Expected 1 event for numRecentEvents=-1, got " + session.events().size());
    // Should be the last event added (event-5)
    assertEquals(
        "event-5",
        session.events().get(0).id(),
        "Expected most recent event (event-5), got " + session.events().get(0).id());
  }

  @Test
  public void testSetAppState_roundTrip() {
    Map<String, Object> appState = Map.of("config", "postgres-value", "version", 42);
    sessionService.setAppState(TEST_APP_NAME, appState).blockingAwait();

    Map<String, Object> retrieved = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(retrieved);
    assertEquals("postgres-value", retrieved.get("config"));
    assertEquals(42, retrieved.get("version"));
  }

  @Test
  public void testSetUserState_roundTrip() {
    Map<String, Object> userState = Map.of("theme", "dark", "lang", "en");
    sessionService.setUserState(TEST_APP_NAME, TEST_USER_ID, userState).blockingAwait();

    Map<String, Object> retrieved =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(retrieved);
    assertEquals("dark", retrieved.get("theme"));
    assertEquals("en", retrieved.get("lang"));
  }

  @Test
  public void testSetAppState_overwritesExisting() {
    Map<String, Object> first = Map.of("key1", "a", "key2", "b");
    sessionService.setAppState(TEST_APP_NAME, first).blockingAwait();

    Map<String, Object> second = Map.of("key3", "c");
    sessionService.setAppState(TEST_APP_NAME, second).blockingAwait();

    Map<String, Object> retrieved = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(retrieved);
    assertEquals("c", retrieved.get("key3"));
    assertFalse(retrieved.containsKey("key1"), "key1 should be gone after full replacement");
    assertFalse(retrieved.containsKey("key2"), "key2 should be gone after full replacement");
  }

  @Test
  public void testSetAppState_visibleViaGetSession() {
    Map<String, Object> appState = Map.of("setting", "global-pg");
    sessionService.setAppState(TEST_APP_NAME, appState).blockingAwait();

    String sessionId = "postgres-app-vis-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(session);
    assertEquals("global-pg", session.state().get(State.APP_PREFIX + "setting"));
  }

  @Test
  public void testSetUserState_visibleViaGetSession() {
    Map<String, Object> userState = Map.of("pref", "user-pg");
    sessionService.setUserState(TEST_APP_NAME, TEST_USER_ID, userState).blockingAwait();

    String sessionId = "postgres-user-vis-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(session);
    assertEquals("user-pg", session.state().get(State.USER_PREFIX + "pref"));
  }

  @Test
  public void testGetAppState_afterAppendEventDelta() {
    String sessionId = "postgres-appstate-delta-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.APP_PREFIX + "fromEvent", "eventValue");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-author")
            .content(Content.fromParts(Part.fromText("delta event")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(session, event).blockingGet();

    Map<String, Object> appState = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(appState);
    assertEquals("eventValue", appState.get("fromEvent"));
  }

  @Test
  public void testListSessions_includesMergedState() {
    String sessionId = "postgres-list-merged-" + System.currentTimeMillis();
    ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();
    initialState.put(State.APP_PREFIX + "appKey", "appVal");
    initialState.put(State.USER_PREFIX + "userKey", "userVal");
    initialState.put("sessionKey", "sessionVal");

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, initialState, sessionId)
        .blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertNotNull(response);
    assertEquals(1, response.sessions().size());

    Session listed = response.sessions().get(0);
    assertEquals("appVal", listed.state().get(State.APP_PREFIX + "appKey"));
    assertEquals("userVal", listed.state().get(State.USER_PREFIX + "userKey"));
    assertEquals("sessionVal", listed.state().get("sessionKey"));
    assertTrue(listed.events().isEmpty(), "listSessions should not include events");
  }

  @Test
  public void testListSessions_reflectsAppendEventChanges() {
    String sessionId = "postgres-list-delta-" + System.currentTimeMillis();
    ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();
    initialState.put(State.APP_PREFIX + "counter", "before");
    initialState.put("local", "original");

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, initialState, sessionId)
        .blockingGet();

    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.APP_PREFIX + "counter", "after");
    delta.put("local", "updated");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-author")
            .content(Content.fromParts(Part.fromText("update")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(session, event).blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    Session listed = response.sessions().get(0);

    assertEquals("after", listed.state().get(State.APP_PREFIX + "counter"));
    assertEquals("updated", listed.state().get("local"));
  }

  @Test
  public void testStateRemoved_viaAppendEvent() {
    String sessionId = "postgres-removed-" + System.currentTimeMillis();
    ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();
    initialState.put(State.APP_PREFIX + "removeMe", "app-gone");
    initialState.put(State.USER_PREFIX + "removeMe", "user-gone");
    initialState.put("removeMe", "session-gone");

    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, initialState, sessionId)
            .blockingGet();

    // Verify initial state is present
    assertEquals("app-gone", session.state().get(State.APP_PREFIX + "removeMe"));
    assertEquals("user-gone", session.state().get(State.USER_PREFIX + "removeMe"));
    assertEquals("session-gone", session.state().get("removeMe"));

    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.APP_PREFIX + "removeMe", State.REMOVED);
    delta.put(State.USER_PREFIX + "removeMe", State.REMOVED);
    delta.put("removeMe", State.REMOVED);

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-author")
            .content(Content.fromParts(Part.fromText("remove keys")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    Session fresh =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(fresh, event).blockingGet();

    Session updated =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(updated);
    assertFalse(updated.state().containsKey(State.APP_PREFIX + "removeMe"));
    assertFalse(updated.state().containsKey(State.USER_PREFIX + "removeMe"));
    assertFalse(updated.state().containsKey("removeMe"));
  }

  @Test
  public void testAppendEvent_contentAndAuthorPreserved() {
    String sessionId = "postgres-event-rt-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    String eventId = UUID.randomUUID().toString();
    Event event =
        Event.builder()
            .id(eventId)
            .author("user")
            .content(Content.fromParts(Part.fromText("Hello from the user")))
            .timestamp(Instant.now().toEpochMilli())
            .build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(session, event).blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals(1, retrieved.events().size());

    Event retrievedEvent = retrieved.events().get(0);
    assertEquals(eventId, retrievedEvent.id());
    assertEquals("user", retrievedEvent.author());
    assertTrue(retrievedEvent.content().isPresent());
    assertTrue(retrievedEvent.content().get().parts().isPresent());
    assertFalse(retrievedEvent.content().get().parts().get().isEmpty());
    assertEquals(
        "Hello from the user", retrievedEvent.content().get().parts().get().get(0).text().get());
  }

  @Test
  public void testDeleteSession_preservesAppAndUserState() {
    String sessionId = "postgres-del-preserve-" + System.currentTimeMillis();
    ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();
    initialState.put(State.APP_PREFIX + "survive", "app-lives");
    initialState.put(State.USER_PREFIX + "survive", "user-lives");
    initialState.put("sessionOnly", "will-die");

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, initialState, sessionId)
        .blockingGet();

    sessionService.deleteSession(TEST_APP_NAME, TEST_USER_ID, sessionId).blockingAwait();

    // Session should be gone
    assertNull(
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet());

    // App and user state should survive
    Map<String, Object> appState = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(appState, "App state should survive session deletion");
    assertEquals("app-lives", appState.get("survive"));

    Map<String, Object> userState =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(userState, "User state should survive session deletion");
    assertEquals("user-lives", userState.get("survive"));
  }

  @Test
  public void testMultiTurnToolConversation() {
    String sessionId = "postgres-multi-turn-" + System.currentTimeMillis();

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    long baseTimestamp = Instant.now().toEpochMilli();

    Event userMessage =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("user")
            .content(Content.fromParts(Part.fromText("What's the weather in Tokyo?")))
            .timestamp(baseTimestamp)
            .build();

    Event modelFunctionCall =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("model")
            .content(
                Content.fromParts(
                    Part.builder()
                        .functionCall(
                            FunctionCall.builder()
                                .name("get_weather")
                                .args(Map.of("city", "Tokyo"))
                                .id("weather-1")
                                .build())
                        .build()))
            .timestamp(baseTimestamp + 100)
            .build();

    Event toolResponse =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("tool")
            .content(
                Content.fromParts(
                    Part.builder()
                        .functionResponse(
                            FunctionResponse.builder()
                                .name("get_weather")
                                .response(Map.of("temp", 18, "condition", "cloudy"))
                                .id("weather-1")
                                .build())
                        .build()))
            .timestamp(baseTimestamp + 200)
            .build();

    Event modelFinalResponse =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("model")
            .content(Content.fromParts(Part.fromText("The weather in Tokyo is 18°C and cloudy.")))
            .timestamp(baseTimestamp + 300)
            .build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(session, userMessage).blockingGet();

    session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(session, modelFunctionCall).blockingGet();

    session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(session, toolResponse).blockingGet();

    session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(session, modelFinalResponse).blockingGet();

    Session retrievedSession =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertEquals(4, retrievedSession.events().size());

    assertEquals(
        "What's the weather in Tokyo?",
        retrievedSession.events().get(0).content().get().parts().get().get(0).text().get());

    FunctionCall retrievedCall =
        retrievedSession.events().get(1).content().get().parts().get().get(0).functionCall().get();
    assertEquals("get_weather", retrievedCall.name().get());
    assertEquals("Tokyo", retrievedCall.args().get().get("city"));

    FunctionResponse retrievedResponse =
        retrievedSession
            .events()
            .get(2)
            .content()
            .get()
            .parts()
            .get()
            .get(0)
            .functionResponse()
            .get();
    assertEquals("get_weather", retrievedResponse.name().get());
    assertEquals(18, retrievedResponse.response().get().get("temp"));

    assertEquals(
        "The weather in Tokyo is 18°C and cloudy.",
        retrievedSession.events().get(3).content().get().parts().get().get(0).text().get());
  }

  @Test
  public void testMixedPartsInSingleEvent() {
    String sessionId = "postgres-mixed-parts-" + System.currentTimeMillis();

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    Part textPart = Part.fromText("Let me call a function:");
    Part functionCallPart =
        Part.builder()
            .functionCall(
                FunctionCall.builder()
                    .name("calculate")
                    .args(Map.of("expression", "2+2"))
                    .id("calc-1")
                    .build())
            .build();
    Part fileDataPart =
        Part.builder()
            .fileData(
                FileData.builder().fileUri("gs://bucket/data.csv").mimeType("text/csv").build())
            .build();

    Event originalEvent =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("model")
            .content(Content.fromParts(textPart, functionCallPart, fileDataPart))
            .timestamp(Instant.now().toEpochMilli())
            .build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    sessionService.appendEvent(session, originalEvent).blockingGet();

    Session retrievedSession =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    Event retrievedEvent = retrievedSession.events().get(0);
    List<Part> parts = retrievedEvent.content().get().parts().get();

    assertEquals(3, parts.size());

    assertTrue(parts.get(0).text().isPresent());
    assertEquals("Let me call a function:", parts.get(0).text().get());

    assertTrue(parts.get(1).functionCall().isPresent());
    assertEquals("calculate", parts.get(1).functionCall().get().name().get());

    assertTrue(parts.get(2).fileData().isPresent());
    assertEquals("gs://bucket/data.csv", parts.get(2).fileData().get().fileUri().get());
  }

  @Test
  public void testFilterByNumRecentEvents() throws InterruptedException {
    String sessionId = "postgres-recent-events-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    for (int i = 1; i <= 10; i++) {
      Event event =
          Event.builder()
              .id("event-" + i)
              .author("test-author")
              .content(Content.fromParts(Part.fromText("Event " + i)))
              .timestamp(Instant.now().toEpochMilli())
              .build();

      Session session =
          sessionService
              .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
              .blockingGet();

      sessionService.appendEvent(session, event).blockingGet();
      TimeUnit.MILLISECONDS.sleep(10);
    }

    GetSessionConfig config = GetSessionConfig.builder().numRecentEvents(3).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(3, session.events().size());
    assertEquals("event-8", session.events().get(0).id());
    assertEquals("event-9", session.events().get(1).id());
    assertEquals("event-10", session.events().get(2).id());
  }

  @Test
  public void testFilterByAfterTimestamp() {
    String sessionId = "postgres-timestamp-filter-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    Instant startTime = Instant.now();

    for (int i = 1; i <= 5; i++) {
      Event event =
          Event.builder()
              .id("event-" + i)
              .author("test-author")
              .content(Content.fromParts(Part.fromText("Event " + i)))
              .timestamp(startTime.plusSeconds(i).toEpochMilli())
              .build();

      Session session =
          sessionService
              .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
              .blockingGet();

      sessionService.appendEvent(session, event).blockingGet();
    }

    GetSessionConfig config =
        GetSessionConfig.builder().afterTimestamp(startTime.plusSeconds(3)).build();
    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.of(config))
            .blockingGet();

    assertNotNull(session);
    assertEquals(2, session.events().size());
    assertEquals("event-4", session.events().get(0).id());
    assertEquals("event-5", session.events().get(1).id());
  }

  @Test
  public void testListEvents() throws InterruptedException {
    String sessionId = "postgres-list-events-" + System.currentTimeMillis();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    for (int i = 1; i <= 5; i++) {
      Event event =
          Event.builder()
              .id("event-" + i)
              .author("test-author")
              .content(Content.fromParts(Part.fromText("index: " + i)))
              .timestamp(Instant.now().toEpochMilli())
              .build();

      Session session =
          sessionService
              .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
              .blockingGet();
      sessionService.appendEvent(session, event).blockingGet();
      TimeUnit.MILLISECONDS.sleep(10);
    }

    ListEventsResponse response =
        sessionService.listEvents(TEST_APP_NAME, TEST_USER_ID, sessionId).blockingGet();

    assertNotNull(response);
    assertEquals(5, response.events().size());
    assertEquals("event-1", response.events().get(0).id());
    assertEquals("event-5", response.events().get(4).id());
  }

  @Test
  public void testAppendEventWithNullContent() {
    String sessionId = "postgres-null-content-" + System.currentTimeMillis();

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    Event emptyContentEvent =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("system")
            .timestamp(Instant.now().toEpochMilli())
            .build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    sessionService.appendEvent(session, emptyContentEvent).blockingGet();

    Session retrievedSession =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(retrievedSession);
    assertEquals(1, retrievedSession.events().size());

    Event retrievedEvent = retrievedSession.events().get(0);
    assertTrue(
        retrievedEvent.content().isEmpty() || retrievedEvent.content().get().parts().isEmpty());
  }

  @Test
  public void testTempStateIsIgnored() {
    String sessionId = "postgres-temp-state-" + System.currentTimeMillis();

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    ConcurrentHashMap<String, Object> stateDelta = new ConcurrentHashMap<>();
    stateDelta.put("temp:scratch", "ignored");
    stateDelta.put("persisted", "kept");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test-author")
            .content(Content.fromParts(Part.fromText("event with temp state")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(stateDelta).build())
            .build();

    Session session =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    sessionService.appendEvent(session, event).blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals("kept", retrieved.state().get("persisted"));
    assertNull(retrieved.state().get("temp:scratch"));
  }
}
