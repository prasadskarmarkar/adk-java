package com.google.adk.sessions;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.adk.events.Event;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/**
 * Tests for the close() method behavior: idempotency, thread safety, and IllegalStateException
 * after close for all public operations.
 */
public class CloseMethodTest {

  private static final String TEST_DB_URL =
      "jdbc:h2:mem:close_test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
  private static final String TEST_APP_NAME = "close-test-app";
  private static final String TEST_USER_ID = "close-test-user";

  @Test
  public void testClose_idempotent() {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);

    // First close should succeed
    assertDoesNotThrow(service::close);

    // Second close should also succeed (no-op)
    assertDoesNotThrow(service::close);

    // Third close for good measure
    assertDoesNotThrow(service::close);
  }

  @Test
  public void testClose_concurrentCloseCallsAreIdempotent() throws InterruptedException {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);

    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(service::close);
    }

    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }

    // Should not throw - already closed
    assertDoesNotThrow(service::close);
  }

  @Test
  public void testCreateSession_afterClose_throwsIllegalStateException() {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);
    service.close();

    assertThrows(
        IllegalStateException.class,
        () ->
            service
                .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "s1")
                .blockingGet());
  }

  @Test
  public void testGetSession_afterClose_throwsIllegalStateException() {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);
    service.close();

    assertThrows(
        IllegalStateException.class,
        () ->
            service.getSession(TEST_APP_NAME, TEST_USER_ID, "s1", Optional.empty()).blockingGet());
  }

  @Test
  public void testListSessions_afterClose_throwsIllegalStateException() {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);
    service.close();

    assertThrows(
        IllegalStateException.class,
        () -> service.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet());
  }

  @Test
  public void testDeleteSession_afterClose_throwsIllegalStateException() {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);
    service.close();

    assertThrows(
        IllegalStateException.class,
        () -> service.deleteSession(TEST_APP_NAME, TEST_USER_ID, "s1").blockingAwait());
  }

  @Test
  public void testListEvents_afterClose_throwsIllegalStateException() {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);
    service.close();

    assertThrows(
        IllegalStateException.class,
        () -> service.listEvents(TEST_APP_NAME, TEST_USER_ID, "s1").blockingGet());
  }

  @Test
  public void testAppendEvent_afterClose_throwsIllegalStateException() {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);
    service.close();

    Session fakeSession =
        Session.builder("s1")
            .appName(TEST_APP_NAME)
            .userId(TEST_USER_ID)
            .state(new ConcurrentHashMap<>())
            .events(new ArrayList<>())
            .build();

    Event event =
        Event.builder()
            .id(UUID.randomUUID().toString())
            .author("test")
            .content(Content.fromParts(Part.fromText("test")))
            .timestamp(Instant.now().toEpochMilli())
            .build();

    assertThrows(
        IllegalStateException.class, () -> service.appendEvent(fakeSession, event).blockingGet());
  }

  @Test
  public void testOperationsWorkBeforeClose_thenFailAfter() {
    DatabaseSessionService service = new DatabaseSessionService(TEST_DB_URL);

    // Operations should work before close
    Session session =
        service
            .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "pre-close")
            .blockingGet();

    Session retrieved =
        service
            .getSession(TEST_APP_NAME, TEST_USER_ID, "pre-close", Optional.empty())
            .blockingGet();

    // Close the service
    service.close();

    // All operations should now fail
    assertThrows(
        IllegalStateException.class,
        () ->
            service
                .createSession(TEST_APP_NAME, TEST_USER_ID, new ConcurrentHashMap<>(), "post-close")
                .blockingGet());

    assertThrows(
        IllegalStateException.class,
        () ->
            service
                .getSession(TEST_APP_NAME, TEST_USER_ID, "pre-close", Optional.empty())
                .blockingGet());

    assertThrows(
        IllegalStateException.class,
        () -> service.listSessions(TEST_APP_NAME, TEST_USER_ID).blockingGet());

    assertThrows(
        IllegalStateException.class,
        () -> service.deleteSession(TEST_APP_NAME, TEST_USER_ID, "pre-close").blockingAwait());

    assertThrows(
        IllegalStateException.class,
        () -> service.listEvents(TEST_APP_NAME, TEST_USER_ID, "pre-close").blockingGet());
  }
}
