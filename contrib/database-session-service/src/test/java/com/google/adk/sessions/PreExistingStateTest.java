package com.google.adk.sessions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for session creation and retrieval when app/user state already exists from prior sessions,
 * setAppState, or setUserState calls. Verifies correct state visibility and merging.
 */
public class PreExistingStateTest {

  private static final String TEST_DB_URL =
      "jdbc:h2:mem:preexisting_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
  private static final String TEST_APP_NAME = "preexist-app";
  private static final String TEST_USER_ID = "preexist-user";

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

  @Test
  public void testNewSession_seesPreExistingAppState() {
    // Set app state before any session exists
    sessionService.setAppState(TEST_APP_NAME, Map.of("config", "global_value")).blockingAwait();

    // Create a session without any app: keys
    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "s1")
            .blockingGet();

    // The created session should see the pre-existing app state
    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "s1", Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals("global_value", retrieved.state().get(State.APP_PREFIX + "config"));
  }

  @Test
  public void testNewSession_seesPreExistingUserState() {
    // Set user state before any session exists
    sessionService
        .setUserState(TEST_APP_NAME, TEST_USER_ID, Map.of("theme", "dark"))
        .blockingAwait();

    Session session =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "s2")
            .blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "s2", Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals("dark", retrieved.state().get(State.USER_PREFIX + "theme"));
  }

  @Test
  public void testNewSession_seesPreExistingAppAndUserState() {
    sessionService.setAppState(TEST_APP_NAME, Map.of("app_key", "app_val")).blockingAwait();
    sessionService
        .setUserState(TEST_APP_NAME, TEST_USER_ID, Map.of("user_key", "user_val"))
        .blockingAwait();

    ConcurrentHashMap<String, Object> sessionState = new ConcurrentHashMap<>();
    sessionState.put("session_key", "session_val");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, sessionState, "s3").blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "s3", Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals("app_val", retrieved.state().get(State.APP_PREFIX + "app_key"));
    assertEquals("user_val", retrieved.state().get(State.USER_PREFIX + "user_key"));
    assertEquals("session_val", retrieved.state().get("session_key"));
  }

  @Test
  public void testSecondSession_seesAppStateFromFirstSession() {
    // First session creates app state
    ConcurrentHashMap<String, Object> state1 = new ConcurrentHashMap<>();
    state1.put(State.APP_PREFIX + "created_by", "session1");
    state1.put(State.APP_PREFIX + "shared_config", "original");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state1, "first").blockingGet();

    // Second session with different app state - should merge
    ConcurrentHashMap<String, Object> state2 = new ConcurrentHashMap<>();
    state2.put(State.APP_PREFIX + "added_by", "session2");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state2, "second").blockingGet();

    // Both sessions should see merged app state
    Session first =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "first", Optional.empty())
            .blockingGet();
    Session second =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "second", Optional.empty())
            .blockingGet();

    assertNotNull(first);
    assertNotNull(second);

    // Both see session1's key
    assertEquals("session1", first.state().get(State.APP_PREFIX + "created_by"));
    assertEquals("session1", second.state().get(State.APP_PREFIX + "created_by"));

    // Both see session2's key
    assertEquals("session2", first.state().get(State.APP_PREFIX + "added_by"));
    assertEquals("session2", second.state().get(State.APP_PREFIX + "added_by"));

    // Both see the original shared config
    assertEquals("original", first.state().get(State.APP_PREFIX + "shared_config"));
    assertEquals("original", second.state().get(State.APP_PREFIX + "shared_config"));
  }

  @Test
  public void testSecondSession_seesUserStateFromFirstSession() {
    ConcurrentHashMap<String, Object> state1 = new ConcurrentHashMap<>();
    state1.put(State.USER_PREFIX + "pref1", "from_session1");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state1, "us1").blockingGet();

    ConcurrentHashMap<String, Object> state2 = new ConcurrentHashMap<>();
    state2.put(State.USER_PREFIX + "pref2", "from_session2");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state2, "us2").blockingGet();

    Session s1 =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "us1", Optional.empty())
            .blockingGet();
    Session s2 =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "us2", Optional.empty())
            .blockingGet();

    // Both sessions see both user state keys
    assertEquals("from_session1", s1.state().get(State.USER_PREFIX + "pref1"));
    assertEquals("from_session2", s1.state().get(State.USER_PREFIX + "pref2"));
    assertEquals("from_session1", s2.state().get(State.USER_PREFIX + "pref1"));
    assertEquals("from_session2", s2.state().get(State.USER_PREFIX + "pref2"));
  }

  @Test
  public void testAppendEvent_modifiesAppState_visibleToOtherSessions() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.APP_PREFIX + "counter", 0);

    Session s1 =
        sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, "event-s1").blockingGet();

    sessionService
        .createSession(TEST_APP_NAME, "other-user", new ConcurrentHashMap<>(), "event-s2")
        .blockingGet();

    // Append event that modifies app state via s1
    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.APP_PREFIX + "counter", 42);

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("update counter")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    sessionService.appendEvent(s1, event).blockingGet();

    // s2 (different user) should see the updated app state
    Session s2Retrieved =
        sessionService
            .getSession(TEST_APP_NAME, "other-user", "event-s2", Optional.empty())
            .blockingGet();

    assertNotNull(s2Retrieved);
    assertEquals(42, s2Retrieved.state().get(State.APP_PREFIX + "counter"));
  }

  @Test
  public void testAppendEvent_modifiesUserState_visibleToOtherSessionsSameUser() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.USER_PREFIX + "visits", 1);

    Session s1 =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, state, "user-evt-s1")
            .blockingGet();

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "user-evt-s2")
        .blockingGet();

    // Modify user state via appendEvent on s1
    ConcurrentHashMap<String, Object> delta = new ConcurrentHashMap<>();
    delta.put(State.USER_PREFIX + "visits", 99);

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("update visits")))
            .timestamp(Instant.now().toEpochMilli())
            .actions(EventActions.builder().stateDelta(delta).build())
            .build();

    sessionService.appendEvent(s1, event).blockingGet();

    // s2 (same user) should see updated user state
    Session s2Retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "user-evt-s2", Optional.empty())
            .blockingGet();

    assertNotNull(s2Retrieved);
    assertEquals(99, s2Retrieved.state().get(State.USER_PREFIX + "visits"));
  }

  @Test
  public void testDeleteSession_doesNotDeleteAppState() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.APP_PREFIX + "persist_me", "should_survive");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, "delete-me").blockingGet();

    sessionService.deleteSession(TEST_APP_NAME, TEST_USER_ID, "delete-me").blockingAwait();

    // App state should still exist
    Map<String, Object> appState = sessionService.getAppState(TEST_APP_NAME).blockingGet();
    assertNotNull(appState);
    assertEquals("should_survive", appState.get("persist_me"));
  }

  @Test
  public void testDeleteSession_doesNotDeleteUserState() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.USER_PREFIX + "persist_me", "should_survive");

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, state, "delete-me-user")
        .blockingGet();

    sessionService.deleteSession(TEST_APP_NAME, TEST_USER_ID, "delete-me-user").blockingAwait();

    Map<String, Object> userState =
        sessionService.getUserState(TEST_APP_NAME, TEST_USER_ID).blockingGet();
    assertNotNull(userState);
    assertEquals("should_survive", userState.get("persist_me"));
  }

  @Test
  public void testNewSession_afterDeletedSession_stillSeesAppUserState() {
    ConcurrentHashMap<String, Object> state = new ConcurrentHashMap<>();
    state.put(State.APP_PREFIX + "app_data", "app_val");
    state.put(State.USER_PREFIX + "user_data", "user_val");

    sessionService.createSession(TEST_APP_NAME, TEST_USER_ID, state, "old-session").blockingGet();

    sessionService.deleteSession(TEST_APP_NAME, TEST_USER_ID, "old-session").blockingAwait();

    // Create new session - should still see app and user state from deleted session
    Session newSession =
        sessionService
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "new-session")
            .blockingGet();

    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "new-session", Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    assertEquals("app_val", retrieved.state().get(State.APP_PREFIX + "app_data"));
    assertEquals("user_val", retrieved.state().get(State.USER_PREFIX + "user_data"));
  }

  @Test
  public void testSetAppState_thenCreateSession_withOverlappingKeys() {
    // Pre-set app state
    sessionService
        .setAppState(TEST_APP_NAME, Map.of("key1", "original", "key2", "keep"))
        .blockingAwait();

    // Create session with overlapping app key
    ConcurrentHashMap<String, Object> sessionState = new ConcurrentHashMap<>();
    sessionState.put(State.APP_PREFIX + "key1", "overwritten");
    sessionState.put(State.APP_PREFIX + "key3", "new");

    sessionService
        .createSession(TEST_APP_NAME, TEST_USER_ID, sessionState, "overlap")
        .blockingGet();

    // Verify the merged result
    Session retrieved =
        sessionService
            .getSession(TEST_APP_NAME, TEST_USER_ID, "overlap", Optional.empty())
            .blockingGet();

    assertNotNull(retrieved);
    // key1 should be overwritten by createSession
    assertEquals("overwritten", retrieved.state().get(State.APP_PREFIX + "key1"));
    // key2 should be preserved from setAppState
    assertEquals("keep", retrieved.state().get(State.APP_PREFIX + "key2"));
    // key3 should be new from createSession
    assertEquals("new", retrieved.state().get(State.APP_PREFIX + "key3"));
  }
}
