package com.google.adk.sessions;

import java.util.Map;

/**
 * Verifies the fix for Issue #2: getAppState/getUserState now return Maybe instead of Single.
 *
 * <p>Before the fix, these methods used Single which threw NullPointerException when no state
 * existed. After the fix, they use Maybe which completes empty (returns null from blockingGet).
 *
 * <p>Usage: Run the main method. No external database needed (uses in-memory H2).
 */
public class ReproduceNullSingleIssue {

  public static void main(String[] args) {
    String jdbcUrl = "jdbc:h2:mem:reproduce_null;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";

    try (DatabaseSessionService service = new DatabaseSessionService(jdbcUrl)) {

      // --- Test 1: getAppState for a non-existent app ---
      System.out.println("=== Test 1: getAppState when no app state exists ===");
      Map<String, Object> appState = service.getAppState("non-existent-app").blockingGet();
      if (appState == null) {
        System.out.println("FIX VERIFIED: Maybe completed empty, blockingGet returned null.");
        System.out.println("  No NullPointerException thrown.");
      } else {
        System.out.println("UNEXPECTED: Got non-null state: " + appState);
      }

      System.out.println();

      // --- Test 2: getUserState for a non-existent user ---
      System.out.println("=== Test 2: getUserState when no user state exists ===");
      Map<String, Object> userState =
          service.getUserState("non-existent-app", "non-existent-user").blockingGet();
      if (userState == null) {
        System.out.println("FIX VERIFIED: Maybe completed empty, blockingGet returned null.");
        System.out.println("  No NullPointerException thrown.");
      } else {
        System.out.println("UNEXPECTED: Got non-null state: " + userState);
      }

      System.out.println();

      // --- Test 3: getAppState AFTER setting state should still work ---
      System.out.println("=== Test 3: getAppState after setting state (should work) ===");
      service.setAppState("my-app", Map.of("version", "1.0")).blockingAwait();
      Map<String, Object> existingState = service.getAppState("my-app").blockingGet();
      System.out.println("Got app state: " + existingState);
      System.out.println("OK: Works fine when state exists.");

    } catch (Exception e) {
      System.out.println("FAILURE: " + e.getClass().getSimpleName() + ": " + e.getMessage());
      e.printStackTrace();
    }
  }
}
