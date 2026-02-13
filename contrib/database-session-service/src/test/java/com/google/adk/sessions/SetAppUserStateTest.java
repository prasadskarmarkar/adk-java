package com.google.adk.sessions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for setAppState(), setUserState(), getAppState(), and getUserState() methods. These public
 * methods were previously untested.
 */
public class SetAppUserStateTest {

  private static final String TEST_DB_URL =
      "jdbc:h2:mem:set_state_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
  private static final String TEST_APP_NAME = "set-state-app";
  private static final String TEST_USER_ID = "set-state-user";

  private DatabaseSessionService sessionService;

  @BeforeEach
  public void setUp() {
    sessionService = new DatabaseSessionService(TEST_DB_URL);
  }

  @AfterEach
  public void tearDown() {
    if (sessionService != null) {
      sessionService.close();
    }
  }

  // ===== setAppState / getAppState =====

  @Test
  public void testSetAppState_basicRoundTrip() {
    Map<String, Object> state = new HashMap<>();
    state.put("version", "1.0");
    state.put("feature_flags", true);

    sessionService.setAppState(TEST_APP_NAME, state).blockingAwait();

    Map<String, Object> retrieved = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(retrieved);
    assertEquals("1.0", retrieved.get("version"));
    assertEquals(true, retrieved.get("feature_flags"));
  }

  @Test
  public void testSetAppState_overwritesExistingState() {
    Map<String, Object> state1 = new HashMap<>();
    state1.put("key1", "value1");
    state1.put("key2", "value2");

    sessionService.setAppState(TEST_APP_NAME, state1).blockingAwait();

    // Overwrite with completely different state
    Map<String, Object> state2 = new HashMap<>();
    state2.put("key3", "value3");

    sessionService.setAppState(TEST_APP_NAME, state2).blockingAwait();

    Map<String, Object> retrieved = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(retrieved);
    // Old keys should be gone (full replacement)
    assertFalse(retrieved.containsKey("key1"));
    assertFalse(retrieved.containsKey("key2"));
    assertEquals("value3", retrieved.get("key3"));
  }

  @Test
  public void testSetAppState_emptyMap() {
    // Set some state first
    Map<String, Object> state = new HashMap<>();
    state.put("key", "value");
    sessionService.setAppState(TEST_APP_NAME, state).blockingAwait();

    // Overwrite with empty map
    sessionService.setAppState(TEST_APP_NAME, Collections.emptyMap()).blockingAwait();

    Map<String, Object> retrieved = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(retrieved);
    assertTrue(retrieved.isEmpty());
  }

  @Test
  public void testSetAppState_visibleViaGetSession() {
    // Set app state directly
    Map<String, Object> appState = new HashMap<>();
    appState.put("global_config", "enabled");
    sessionService.setAppState(TEST_APP_NAME, appState).blockingAwait();

    // Create a session (no app: state in initial state)
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "s1")
            .blockingGet();

    // getSession should merge the app state with prefix
    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "s1", Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals("enabled", retrieved.state().get(State.APP_PREFIX + "global_config"));
  }

  @Test
  public void testSetAppState_interactionWithAppendEventDelta() {
    // Set initial app state via setAppState
    Map<String, Object> initial = new HashMap<>();
    initial.put("counter", 0);
    initial.put("keep_me", "preserved");
    sessionService.setAppState(TEST_APP_NAME, initial).blockingAwait();

    // Create a session and append event with app state delta
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "s2")
            .blockingGet();

    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.APP_PREFIX + "counter", 1);
    delta.put(State.APP_PREFIX + "new_key", "added");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("update")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    sessionService.appendEvent(session, event).blockingGet();

    // Verify via getAppState (no prefix in returned keys)
    Map<String, Object> appState = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(appState);
    assertEquals(1, appState.get("counter"));
    assertEquals("added", appState.get("new_key"));
    assertEquals("preserved", appState.get("keep_me"));
  }

  @Test
  public void testSetAppState_afterAppendEventOverwritesAll() {
    // Create session with app state via event delta
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "s3")
            .blockingGet();

    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.APP_PREFIX + "from_event", "event_value");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("set state")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    sessionService.appendEvent(session, event).blockingGet();

    // Now overwrite with setAppState
    Map<String, Object> replacement = new HashMap<>();
    replacement.put("replaced", "new_value");

    sessionService.setAppState(TEST_APP_NAME, replacement).blockingAwait();

    Map<String, Object> appState = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(appState);
    assertFalse(appState.containsKey("from_event"), "Old key from event delta should be gone");
    assertEquals("new_value", appState.get("replaced"));
  }

  @Test
  public void testSetAppState_isolatedBetweenApps() {
    String app1 = "app-one";
    String app2 = "app-two";

    Map<String, Object> state1 = Map.of("key", "app1_value");
    Map<String, Object> state2 = Map.of("key", "app2_value");

    sessionService.setAppState(app1, state1).blockingAwait();
    sessionService.setAppState(app2, state2).blockingAwait();

    assertEquals("app1_value", sessionService.getAppState(app1).blockingGet().get("key"));
    assertEquals("app2_value", sessionService.getAppState(app2).blockingGet().get("key"));
  }

  @Test
  public void testSetAppState_complexNestedValues() {
    Map<String, Object> state = new HashMap<>();
    state.put("string", "hello");
    state.put("number", 42);
    state.put("double", 3.14);
    state.put("boolean", true);
    state.put("list", java.util.List.of(1, 2, 3));
    state.put("nested", Map.of("inner_key", "inner_value", "inner_num", 99));

    sessionService.setAppState(TEST_APP_NAME, state).blockingAwait();

    Map<String, Object> retrieved = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(retrieved);
    assertEquals("hello", retrieved.get("string"));
    assertEquals(42, retrieved.get("number"));
    assertEquals(3.14, retrieved.get("double"));
    assertEquals(true, retrieved.get("boolean"));
    assertNotNull(retrieved.get("list"));
    assertNotNull(retrieved.get("nested"));
  }

  // ===== setUserState / getUserState =====

  @Test
  public void testSetUserState_basicRoundTrip() {
    Map<String, Object> state = new HashMap<>();
    state.put("theme", "dark");
    state.put("language", "en");

    sessionService.setUserState(TEST_APP_NAME, TEST_USER_ID, state).blockingAwait();

    Map<String, Object> retrieved =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(retrieved);
    assertEquals("dark", retrieved.get("theme"));
    assertEquals("en", retrieved.get("language"));
  }

  @Test
  public void testSetUserState_overwritesExistingState() {
    Map<String, Object> state1 = Map.of("pref1", "val1", "pref2", "val2");
    sessionService.setUserState(TEST_APP_NAME, TEST_USER_ID, state1).blockingAwait();

    Map<String, Object> state2 = Map.of("pref3", "val3");
    sessionService.setUserState(TEST_APP_NAME, TEST_USER_ID, state2).blockingAwait();

    Map<String, Object> retrieved =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(retrieved);
    assertFalse(retrieved.containsKey("pref1"));
    assertFalse(retrieved.containsKey("pref2"));
    assertEquals("val3", retrieved.get("pref3"));
  }

  @Test
  public void testSetUserState_emptyMap() {
    Map<String, Object> state = Map.of("key", "value");
    sessionService.setUserState(TEST_APP_NAME, TEST_USER_ID, state).blockingAwait();

    sessionService
        .setUserState(TEST_APP_NAME, TEST_USER_ID, Collections.emptyMap())
        .blockingAwait();

    Map<String, Object> retrieved =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(retrieved);
    assertTrue(retrieved.isEmpty());
  }

  @Test
  public void testSetUserState_visibleViaGetSession() {
    Map<String, Object> userState = Map.of("pref", "value");
    sessionService.setUserState(TEST_APP_NAME, TEST_USER_ID, userState).blockingAwait();

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "us1")
        .blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "us1", Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals("value", retrieved.state().get(State.USER_PREFIX + "pref"));
  }

  @Test
  public void testSetUserState_interactionWithAppendEventDelta() {
    Map<String, Object> initial = Map.of("counter", 0, "keep_me", "preserved");
    sessionService.setUserState(TEST_APP_NAME, TEST_USER_ID, initial).blockingAwait();

    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "us2")
            .blockingGet();

    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.USER_PREFIX + "counter", 1);
    delta.put(State.USER_PREFIX + "new_key", "added");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("update")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    sessionService.appendEvent(session, event).blockingGet();

    Map<String, Object> userState =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(userState);
    assertEquals(1, userState.get("counter"));
    assertEquals("added", userState.get("new_key"));
    assertEquals("preserved", userState.get("keep_me"));
  }

  @Test
  public void testSetUserState_isolatedBetweenUsers() {
    String user1 = "user-one";
    String user2 = "user-two";

    sessionService
        .setUserState(TEST_APP_NAME, user1, Map.of("pref", "user1_value"))
        .blockingAwait();
    sessionService
        .setUserState(TEST_APP_NAME, user2, Map.of("pref", "user2_value"))
        .blockingAwait();

    assertEquals(
        "user1_value", sessionService.getUserState(TEST_APP_NAME, user1).blockingGet().get("pref"));
    assertEquals(
        "user2_value", sessionService.getUserState(TEST_APP_NAME, user2).blockingGet().get("pref"));
  }

  @Test
  public void testSetUserState_isolatedBetweenApps() {
    String app1 = "user-app-one";
    String app2 = "user-app-two";

    sessionService.setUserState(app1, TEST_USER_ID, Map.of("pref", "app1_value")).blockingAwait();
    sessionService.setUserState(app2, TEST_USER_ID, Map.of("pref", "app2_value")).blockingAwait();

    assertEquals(
        "app1_value", sessionService.getUserState(app1, TEST_USER_ID).blockingGet().get("pref"));
    assertEquals(
        "app2_value", sessionService.getUserState(app2, TEST_USER_ID).blockingGet().get("pref"));
  }

  @Test
  public void testSetUserState_afterAppendEventOverwritesAll() {
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "us3")
            .blockingGet();

    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.USER_PREFIX + "from_event", "event_value");

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("set state")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    sessionService.appendEvent(session, event).blockingGet();

    // Overwrite with setUserState
    sessionService
        .setUserState(TEST_APP_NAME, TEST_USER_ID, Map.of("replaced", "new_value"))
        .blockingAwait();

    Map<String, Object> userState =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(userState);
    assertFalse(userState.containsKey("from_event"));
    assertEquals("new_value", userState.get("replaced"));
  }

  // ===== getAppState / getUserState comprehensive =====

  @Test
  public void testGetAppState_nonExistentApp_returnsNull() {
    assertNull(sessionService.getAppState("no-such-app").blockingGet());
  }

  @Test
  public void testGetUserState_nonExistentUser_returnsNull() {
    assertNull(sessionService.getUserState("no-such-app", "no-such-user").blockingGet());
  }

  @Test
  public void testGetAppState_afterCreateSessionWithAppPrefix() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.APP_PREFIX + "created_via_session", "session_value");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, "get-app-s1").blockingGet();

    // getAppState should return state without prefix
    Map<String, Object> appState = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(appState);
    assertEquals("session_value", appState.get("created_via_session"));
  }

  @Test
  public void testGetUserState_afterCreateSessionWithUserPrefix() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.USER_PREFIX + "created_via_session", "session_value");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, "get-user-s1").blockingGet();

    Map<String, Object> userState =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(userState);
    assertEquals("session_value", userState.get("created_via_session"));
  }

  @Test
  public void testGetAppState_multipleSequentialCalls_consistent() {
    sessionService.setAppState(TEST_APP_NAME, Map.of("key", "stable_value")).blockingAwait();

    for (int i = 0; i < 10; i++) {
      Map<String, Object> state = sessionService.getAppState(TEST_APP_NAME).blockingGet();
      assertNotNull(state);
      assertEquals("stable_value", state.get("key"), "Read #" + i + " should be consistent");
    }
  }

  @Test
  public void testGetUserState_multipleSequentialCalls_consistent() {
    sessionService
        .setUserState(TEST_APP_NAME, TEST_USER_ID, Map.of("key", "stable_value"))
        .blockingAwait();

    for (int i = 0; i < 10; i++) {
      Map<String, Object> state =
          sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
      assertNotNull(state);
      assertEquals("stable_value", state.get("key"), "Read #" + i + " should be consistent");
    }
  }
}
