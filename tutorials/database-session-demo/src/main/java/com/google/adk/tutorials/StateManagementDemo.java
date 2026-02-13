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

package com.google.adk.tutorials;

import com.google.adk.sessions.DatabaseSessionService;
import com.google.adk.sessions.Session;
import com.google.adk.sessions.State;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tutorial demonstrating the 3-tier state system in DatabaseSessionService: - App State: Shared
 * across all users and sessions for an application - User State: Shared across all sessions for a
 * specific user - Session State: Isolated to a specific session
 *
 * <p>This tutorial creates multiple sessions and demonstrates how state is shared and isolated.
 */
public class StateManagementDemo {

  private static final String APP_NAME = "state_demo_app";
  private static final String USER_A = "alice";
  private static final String USER_B = "bob";
  private static final String SESSION_ALPHA = "session-alpha";
  private static final String SESSION_BETA = "session-beta";
  private static final String SESSION_GAMMA = "session-gamma";

  private static final String DB_URL = "jdbc:h2:./demo_state_db;MODE=PostgreSQL";

  public static void main(String[] args) {
    System.out.println("=== DatabaseSessionService State Management Demo ===\n");

    try (DatabaseSessionService sessionService = new DatabaseSessionService(DB_URL)) {

      // ============================================================
      // PHASE 1: Set App-Level State (shared across ALL users/sessions)
      // ============================================================
      System.out.println("PHASE 1: Setting App-Level State");
      System.out.println("----------------------------------");

      Map<String, Object> appState = new HashMap<>();
      appState.put("version", "2.0.0");
      appState.put("feature_flags", Map.of("dark_mode_enabled", true, "beta_features", false));
      appState.put("rate_limit", 100);

      sessionService.setAppState(APP_NAME, appState).blockingAwait();
      System.out.println("✓ App state set: version=2.0.0, rate_limit=100");
      System.out.println();

      // ============================================================
      // PHASE 2: Create Sessions with User and Session State
      // ============================================================
      System.out.println("PHASE 2: Creating Sessions");
      System.out.println("---------------------------");

      // Session 1: User A, Session Alpha
      ConcurrentHashMap<String, Object> session1State = new ConcurrentHashMap<>();
      session1State.put(State.USER_PREFIX + "language", "en");
      session1State.put(State.USER_PREFIX + "theme", "dark");
      session1State.put(State.USER_PREFIX + "notifications_enabled", true);
      session1State.put("current_step", 1);
      session1State.put("conversation_topic", "weather");
      session1State.put("message_count", 0);

      sessionService.createSession(APP_NAME, USER_A, session1State, SESSION_ALPHA).blockingGet();
      System.out.println("✓ Created Session Alpha for User A");
      System.out.println("  - User state: language=en, theme=dark, notifications=true");
      System.out.println("  - Session state: current_step=1, topic=weather, message_count=0");

      // Session 2: User A, Session Beta (same user, different session)
      ConcurrentHashMap<String, Object> session2State = new ConcurrentHashMap<>();
      session2State.put(State.USER_PREFIX + "language", "es"); // Will override to 'es'
      session2State.put("current_step", 5);
      session2State.put("conversation_topic", "sports");
      session2State.put("message_count", 10);

      sessionService.createSession(APP_NAME, USER_A, session2State, SESSION_BETA).blockingGet();
      System.out.println("✓ Created Session Beta for User A");
      System.out.println("  - User state: Updated language=es");
      System.out.println("  - Session state: current_step=5, topic=sports, message_count=10");

      // Session 3: User B, Session Gamma (different user)
      ConcurrentHashMap<String, Object> session3State = new ConcurrentHashMap<>();
      session3State.put(State.USER_PREFIX + "language", "fr");
      session3State.put(State.USER_PREFIX + "theme", "light");
      session3State.put(State.USER_PREFIX + "notifications_enabled", false);
      session3State.put("current_step", 1);
      session3State.put("conversation_topic", "movies");
      session3State.put("message_count", 3);

      sessionService.createSession(APP_NAME, USER_B, session3State, SESSION_GAMMA).blockingGet();
      System.out.println("✓ Created Session Gamma for User B");
      System.out.println("  - User state: language=fr, theme=light, notifications=false");
      System.out.println("  - Session state: current_step=1, topic=movies, message_count=3");
      System.out.println();

      // ============================================================
      // PHASE 3: Retrieve and Verify State Sharing/Isolation
      // ============================================================
      System.out.println("PHASE 3: Verifying State Sharing and Isolation");
      System.out.println("-----------------------------------------------");

      Session alpha =
          sessionService
              .getSession(APP_NAME, USER_A, SESSION_ALPHA, Optional.empty())
              .blockingGet();
      Session beta =
          sessionService.getSession(APP_NAME, USER_A, SESSION_BETA, Optional.empty()).blockingGet();
      Session gamma =
          sessionService
              .getSession(APP_NAME, USER_B, SESSION_GAMMA, Optional.empty())
              .blockingGet();

      System.out.println("\n--- Session Alpha (User A) ---");
      printSessionState(alpha);

      System.out.println("\n--- Session Beta (User A) ---");
      printSessionState(beta);

      System.out.println("\n--- Session Gamma (User B) ---");
      printSessionState(gamma);

      // ============================================================
      // PHASE 4: Verification
      // ============================================================
      System.out.println("\n\nPHASE 4: State Sharing Verification");
      System.out.println("------------------------------------");

      // Verify app state is shared across all sessions
      String alphaVersion = (String) alpha.state().get(State.APP_PREFIX + "version");
      String betaVersion = (String) beta.state().get(State.APP_PREFIX + "version");
      String gammaVersion = (String) gamma.state().get(State.APP_PREFIX + "version");

      if (alphaVersion.equals(betaVersion) && betaVersion.equals(gammaVersion)) {
        System.out.println("✓ App state SHARED: All sessions see version=" + alphaVersion);
      } else {
        System.out.println("✗ App state NOT shared correctly");
      }

      // Verify user state is shared across same user's sessions
      String alphaLang = (String) alpha.state().get(State.USER_PREFIX + "language");
      String betaLang = (String) beta.state().get(State.USER_PREFIX + "language");
      String gammaLang = (String) gamma.state().get(State.USER_PREFIX + "language");

      if (alphaLang.equals(betaLang) && alphaLang.equals("es")) {
        System.out.println("✓ User state SHARED: User A's sessions share language=" + alphaLang);
      } else {
        System.out.println("✗ User A's state not shared correctly");
      }

      if (!gammaLang.equals(alphaLang)) {
        System.out.println("✓ User state ISOLATED: User B has different language=" + gammaLang);
      } else {
        System.out.println("✗ User state not isolated between users");
      }

      // Verify session state is isolated
      Integer alphaStep = (Integer) alpha.state().get("current_step");
      Integer betaStep = (Integer) beta.state().get("current_step");
      Integer gammaStep = (Integer) gamma.state().get("current_step");

      if (!alphaStep.equals(betaStep) && !betaStep.equals(gammaStep)) {
        System.out.println("✓ Session state ISOLATED: Each session has unique current_step");
        System.out.println("  Alpha=" + alphaStep + ", Beta=" + betaStep + ", Gamma=" + gammaStep);
      } else {
        System.out.println("✗ Session state not isolated correctly");
      }

      // ============================================================
      // PHASE 5: State Modification Demo
      // ============================================================
      System.out.println("\n\nPHASE 5: State Modification Demo");
      System.out.println("---------------------------------");

      // Update app state - all sessions should see it
      Map<String, Object> updatedAppState = new HashMap<>();
      updatedAppState.put("version", "2.1.0");
      updatedAppState.put(
          "feature_flags", Map.of("dark_mode_enabled", true, "beta_features", true));
      updatedAppState.put("rate_limit", 150);
      sessionService.setAppState(APP_NAME, updatedAppState).blockingAwait();
      System.out.println("Updated app state: version -> 2.1.0, rate_limit -> 150");

      // Update user state for User A
      Map<String, Object> updatedUserState = new HashMap<>();
      updatedUserState.put("language", "de");
      updatedUserState.put("theme", "dark");
      updatedUserState.put("notifications_enabled", false);
      sessionService.setUserState(APP_NAME, USER_A, updatedUserState).blockingAwait();
      System.out.println("Updated User A state: language -> de, notifications -> false");

      // Retrieve sessions again
      Session alphaUpdated =
          sessionService
              .getSession(APP_NAME, USER_A, SESSION_ALPHA, Optional.empty())
              .blockingGet();
      Session betaUpdated =
          sessionService.getSession(APP_NAME, USER_A, SESSION_BETA, Optional.empty()).blockingGet();
      Session gammaUpdated =
          sessionService
              .getSession(APP_NAME, USER_B, SESSION_GAMMA, Optional.empty())
              .blockingGet();

      System.out.println("\n--- After Updates ---");
      System.out.println(
          "All sessions now see app version: "
              + alphaUpdated.state().get(State.APP_PREFIX + "version"));
      System.out.println(
          "User A sessions see language: "
              + alphaUpdated.state().get(State.USER_PREFIX + "language"));
      System.out.println(
          "User B session still sees language: "
              + gammaUpdated.state().get(State.USER_PREFIX + "language"));
      System.out.println("Session-specific 'current_step' unchanged:");
      System.out.println(
          "  Alpha="
              + alphaUpdated.state().get("current_step")
              + ", Beta="
              + betaUpdated.state().get("current_step")
              + ", Gamma="
              + gammaUpdated.state().get("current_step"));

      // ============================================================
      // Summary
      // ============================================================
      System.out.println("\n\n=== SUMMARY ===");
      System.out.println("App State (app:*):");
      System.out.println("  - Shared across ALL users and sessions");
      System.out.println("  - Use for: global config, feature flags, app version");
      System.out.println("  - Modified via: sessionService.setAppState()");
      System.out.println();
      System.out.println("User State (user:*):");
      System.out.println("  - Shared across all sessions for a specific user");
      System.out.println("  - Use for: user preferences, settings, profile data");
      System.out.println("  - Modified via: sessionService.setUserState()");
      System.out.println();
      System.out.println("Session State (no prefix):");
      System.out.println("  - Isolated to each individual session");
      System.out.println("  - Use for: conversation context, temporary data");
      System.out.println("  - Modified via: state deltas in events");
      System.out.println();
      System.out.println("Demo completed successfully!");

    } catch (Exception e) {
      System.err.println("Error during demo: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static void printSessionState(Session session) {
    System.out.println("App State:");
    session.state().entrySet().stream()
        .filter(e -> e.getKey().startsWith(State.APP_PREFIX))
        .forEach(e -> System.out.println("  " + e.getKey() + " = " + e.getValue()));

    System.out.println("User State:");
    session.state().entrySet().stream()
        .filter(e -> e.getKey().startsWith(State.USER_PREFIX))
        .forEach(e -> System.out.println("  " + e.getKey() + " = " + e.getValue()));

    System.out.println("Session State:");
    session.state().entrySet().stream()
        .filter(
            e ->
                !e.getKey().startsWith(State.APP_PREFIX)
                    && !e.getKey().startsWith(State.USER_PREFIX)
                    && !e.getKey().startsWith(State.TEMP_PREFIX))
        .forEach(e -> System.out.println("  " + e.getKey() + " = " + e.getValue()));
  }
}
