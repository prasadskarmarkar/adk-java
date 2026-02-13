package com.google.adk.sessions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests verifying the listSessions contract: sessions are returned without events but with merged
 * app/user/session state (matching Python DatabaseSessionService parity).
 */
public class ListSessionsContractTest {

  private static final String TEST_DB_URL =
      "jdbc:h2:mem:list_contract_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
  private static final String TEST_APP_NAME = "list-contract-app";
  private static final String TEST_USER_ID = "list-contract-user";

  private DatabaseSessionService sessionService;

  @BeforeEach
  public void setUp() {
    // Clean database before each test to avoid cross-test data leakage
    try (Connection conn = DriverManager.getConnection(TEST_DB_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM events");
      stmt.execute("DELETE FROM sessions");
      stmt.execute("DELETE FROM app_states");
      stmt.execute("DELETE FROM user_states");
    } catch (Exception e) {
      // Tables may not exist yet on first run
    }
    sessionService = new DatabaseSessionService(TEST_DB_URL);
  }

  @AfterEach
  public void tearDown() {
    if (sessionService != null) {
      sessionService.close();
    }
  }

  @Test
  public void testListSessions_returnsSessionsWithoutEvents() {
    String sessionId = "with-events";
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    // Append several events
    for (int i = 0; i < 5; i++) {
      Event event =
          Event.builder()
              .id("event-" + i)
              .author("test")
              .content(Content.fromParts(Part.fromText("Event " + i)))
              .timestamp(Instant.now().toEpochMilli())
              .build();

      Session session =
          sessionService
              .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
              .blockingGet();
      sessionService.appendEvent(session, event).blockingGet();
    }

    // Verify events exist via getSession
    Session full =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    assertEquals(5, full.events().size());

    // listSessions should return the session without events
    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertNotNull(response);
    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);
    assertEquals(sessionId, listed.id());
    assertTrue(listed.events().isEmpty(), "listSessions should not include events");
  }

  @Test
  public void testListSessions_includesAppState() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.APP_PREFIX + "global", "value");
    state.put("local", "session_value");

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, state, "with-app-state")
        .blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertNotNull(response);
    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);

    // App state IS included (merged with prefix)
    assertEquals(
        "value",
        listed.state().get(State.APP_PREFIX + "global"),
        "listSessions should include app-level state");

    // Session-level state IS included
    assertEquals("session_value", listed.state().get("local"));
  }

  @Test
  public void testListSessions_includesUserState() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.USER_PREFIX + "pref", "dark");
    state.put("session_data", "value");

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, state, "with-user-state")
        .blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertNotNull(response);
    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);

    // User state IS included (merged with prefix)
    assertEquals(
        "dark",
        listed.state().get(State.USER_PREFIX + "pref"),
        "listSessions should include user-level state");

    // Session-level state IS included
    assertEquals("value", listed.state().get("session_data"));
  }

  @Test
  public void testListSessions_multipleSessionsAllWithMergedState() {
    for (int i = 0; i < 5; i++) {
      ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
      state.put(State.APP_PREFIX + "config", "value-" + i);
      state.put(State.USER_PREFIX + "pref", "pref-" + i);
      state.put("session_key", "sval-" + i);

      String sid = "multi-" + i;
      Session session =
          sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, sid).blockingGet();

      // Add an event to each session
      Event event =
          Event.builder()
              .id(UUID.randomUUID().toString())
              .author("test")
              .content(Content.fromParts(Part.fromText("Event for " + i)))
              .timestamp(Instant.now().toEpochMilli())
              .actions(EventActions.builder().stateDelta(new ConcurrentHashMap<>()).build())
              .build();
      sessionService.appendEvent(session, event).blockingGet();
    }

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertNotNull(response);
    assertEquals(5, response.sessions().size());

    for (Session listed : response.sessions()) {
      assertTrue(listed.events().isEmpty(), "Session " + listed.id() + " should have no events");
      // App and user state ARE included
      assertNotNull(
          listed.state().get(State.APP_PREFIX + "config"),
          "Session " + listed.id() + " should have app state");
      assertNotNull(
          listed.state().get(State.USER_PREFIX + "pref"),
          "Session " + listed.id() + " should have user state");
      // Session-level state IS present
      assertNotNull(listed.state().get("session_key"));
    }
  }

  @Test
  public void testListSessions_matchesGetSessionState_exceptEvents() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.APP_PREFIX + "global", "app_value");
    state.put(State.USER_PREFIX + "pref", "user_value");
    state.put("local", "session_value");

    String sessionId = "full-vs-listed";
    Session session =
        sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, sessionId).blockingGet();

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("An event")))
            .timestamp(Instant.now().toEpochMilli())
            .build();
    sessionService.appendEvent(session, event).blockingGet();

    // listSessions: no events, but full merged state
    ListSessionsResponse listResponse =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    Session listed = listResponse.sessions().get(0);
    assertTrue(listed.events().isEmpty());
    assertEquals("app_value", listed.state().get(State.APP_PREFIX + "global"));
    assertEquals("user_value", listed.state().get(State.USER_PREFIX + "pref"));
    assertEquals("session_value", listed.state().get("local"));

    // getSession: full state with events
    Session full =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, sessionId, Optional.empty())
            .blockingGet();
    assertNotNull(full);
    assertEquals(1, full.events().size());

    // State should match between listSessions and getSession
    assertEquals(
        listed.state().get(State.APP_PREFIX + "global"),
        full.state().get(State.APP_PREFIX + "global"));
    assertEquals(
        listed.state().get(State.USER_PREFIX + "pref"),
        full.state().get(State.USER_PREFIX + "pref"));
    assertEquals(listed.state().get("local"), full.state().get("local"));
  }

  @Test
  public void testListSessions_includesStateSetViaSetAppState() {
    // Set app state directly (not via createSession)
    sessionService.setAppState(TEST_APP_NAME, Map.of("direct_key", "direct_value")).blockingAwait();

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "via-set")
        .blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);
    assertEquals("direct_value", listed.state().get(State.APP_PREFIX + "direct_key"));
  }

  @Test
  public void testListSessions_includesStateSetViaSetUserState() {
    sessionService
        .setUserState(TEST_APP_NAME, TEST_USER_ID, Map.of("user_direct", "user_val"))
        .blockingAwait();

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "via-set-user")
        .blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);
    assertEquals("user_val", listed.state().get(State.USER_PREFIX + "user_direct"));
  }

  @Test
  public void testListSessions_reflectsStateModifiedViaAppendEvent() {
    ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();
    initialState.put(State.APP_PREFIX + "counter", 0);
    initialState.put(State.USER_PREFIX + "visits", 1);
    initialState.put("step", "start");

    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, initialState, "evt-modified")
            .blockingGet();

    // Modify all three tiers via appendEvent
    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.APP_PREFIX + "counter", 42);
    delta.put(State.USER_PREFIX + "visits", 99);
    delta.put("step", "done");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("update all")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    sessionService.appendEvent(session, event).blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);
    assertEquals(42, listed.state().get(State.APP_PREFIX + "counter"));
    assertEquals(99, listed.state().get(State.USER_PREFIX + "visits"));
    assertEquals("done", listed.state().get("step"));
  }

  @Test
  public void testListSessions_reflectsStateRemovedViaAppendEvent() {
    ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();
    initialState.put(State.APP_PREFIX + "removable", "will_be_removed");
    initialState.put(State.USER_PREFIX + "removable", "will_be_removed");
    initialState.put("removable", "will_be_removed");

    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, initialState, "removed-keys")
            .blockingGet();

    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.APP_PREFIX + "removable", State.REMOVED);
    delta.put(State.USER_PREFIX + "removable", State.REMOVED);
    delta.put("removable", State.REMOVED);

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("remove keys")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    sessionService.appendEvent(session, event).blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);
    assertFalse(listed.state().containsKey(State.APP_PREFIX + "removable"));
    assertFalse(listed.state().containsKey(State.USER_PREFIX + "removable"));
    assertFalse(listed.state().containsKey("removable"));
  }

  @Test
  public void testListSessions_afterDeleteSession_remainingSessionsStillHaveState() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.APP_PREFIX + "shared", "app_val");
    state.put(State.USER_PREFIX + "pref", "user_val");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, "keep-me").blockingGet();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "delete-me")
        .blockingGet();

    sessionService.deleteSession(TEST_APP_NAME, TEST_USER_ID, "delete-me").blockingAwait();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);
    assertEquals("keep-me", listed.id());
    assertEquals("app_val", listed.state().get(State.APP_PREFIX + "shared"));
    assertEquals("user_val", listed.state().get(State.USER_PREFIX + "pref"));
  }

  @Test
  public void testListSessions_sessionOnlyState_noAppOrUserState() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put("only_session", "value");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, "session-only").blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertEquals(1, response.sessions().size());
    Session listed = response.sessions().get(0);
    assertEquals("value", listed.state().get("only_session"));
    // No app/user state keys should exist
    assertTrue(
        listed.state().keySet().stream().noneMatch(k -> k.startsWith(State.APP_PREFIX)),
        "Should have no app: keys when no app state exists");
    assertTrue(
        listed.state().keySet().stream().noneMatch(k -> k.startsWith(State.USER_PREFIX)),
        "Should have no user: keys when no user state exists");
  }

  @Test
  public void testListSessions_appStateSharedAcrossAllListedSessions() {
    // Create multiple sessions, only the first sets app state
    ConcurrentHashMap<String, Object> stateWithApp = new ConcurrentHashMap<>();
    stateWithApp.put(State.APP_PREFIX + "shared_config", "global");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, stateWithApp, "setter").blockingGet();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "reader1")
        .blockingGet();
    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "reader2")
        .blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet();

    assertEquals(3, response.sessions().size());
    // All sessions should have the same app state
    for (Session listed : response.sessions()) {
      assertEquals(
          "global",
          listed.state().get(State.APP_PREFIX + "shared_config"),
          "Session " + listed.id() + " should see shared app state");
    }
  }
}
