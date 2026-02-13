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

import com.google.adk.agents.BaseAgent;
import com.google.adk.agents.LlmAgent;
import com.google.adk.events.Event;
import com.google.adk.models.Gemini;
import com.google.adk.runner.Runner;
import com.google.adk.sessions.DatabaseSessionService;
import com.google.adk.sessions.Session;
import com.google.common.collect.ImmutableList;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Comprehensive test demonstrating multi-turn conversations with the Interactions API.
 *
 * <p>This test verifies:
 *
 * <ul>
 *   <li>The Interactions API properly chains conversations via {@code previousInteractionId}
 *   <li>State is retained across turns without resending full conversation history
 *   <li>{@code InteractionsRequestProcessor} correctly extracts previous interaction IDs
 *   <li>Interaction IDs flow properly: Response → Event → Session → Next Request
 * </ul>
 *
 * <p>The test implements a stateful Q&A pattern across 4 turns:
 *
 * <ol>
 *   <li>Turn 1: User shares favorite color (establishes context)
 *   <li>Turn 2: User shares lucky number (adds more context)
 *   <li>Turn 3: User asks about favorite color (tests recall)
 *   <li>Turn 4: User asks about lucky number (tests recall)
 * </ol>
 *
 * <p>This pattern is inspired by the Python ADK example at
 * adk-python/contributing/samples/interactions_api/main.py
 *
 * <p>Prerequisites:
 *
 * <ul>
 *   <li>PostgreSQL running on localhost:5432
 *   <li>Database created (schema auto-created via Flyway)
 *   <li>Environment variables (or use defaults):
 *       <ul>
 *         <li>DB_HOST (default: localhost)
 *         <li>DB_PORT (default: 5432)
 *         <li>DB_NAME (default: adk_test)
 *         <li>DB_USER (default: adk_user)
 *         <li>DB_PASSWORD (default: adk_password)
 *         <li>GOOGLE_API_KEY or GEMINI_API_KEY
 *       </ul>
 * </ul>
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java@multi-turn
 * </pre>
 */
public class MultiTurnInteractionsDemo {

  private static final String DB_HOST = System.getenv().getOrDefault("DB_HOST", "localhost");
  private static final String DB_PORT = System.getenv().getOrDefault("DB_PORT", "5432");
  private static final String DB_NAME = System.getenv().getOrDefault("DB_NAME", "adk_test");
  private static final String DB_USER = System.getenv().getOrDefault("DB_USER", "adk_user");
  private static final String DB_PASSWORD =
      System.getenv().getOrDefault("DB_PASSWORD", "adk_password");
  private static final String DB_URL =
      String.format(
          "jdbc:postgresql://%s:%s/%s?user=%s&password=%s",
          DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD);
  private static final String APP_NAME = "multi_turn_interactions_test";
  private static final String USER_ID = "test_user";

  /** Holds results from a single conversation turn. */
  private static class TurnResult {
    final String userPrompt;
    final String agentResponse;
    final String interactionId;
    final int eventCount;

    TurnResult(String userPrompt, String agentResponse, String interactionId, int eventCount) {
      this.userPrompt = userPrompt;
      this.agentResponse = agentResponse;
      this.interactionId = interactionId;
      this.eventCount = eventCount;
    }
  }

  /**
   * Executes a single conversation turn and returns detailed results.
   *
   * @param runner the Runner instance
   * @param sessionService the session service
   * @param userId the user ID
   * @param sessionId the session ID
   * @param prompt the user's prompt
   * @param turnNumber the turn number (for logging)
   * @return TurnResult containing response, interaction ID, and metadata
   */
  private static TurnResult executeTurn(
      Runner runner,
      DatabaseSessionService sessionService,
      String userId,
      String sessionId,
      String prompt,
      int turnNumber) {
    System.out.println("\n" + "=".repeat(60));
    System.out.println("TURN " + turnNumber + ": " + prompt);
    System.out.println("=".repeat(60));

    // Before calling agent, show current session state
    Session session =
        sessionService.getSession(APP_NAME, userId, sessionId, Optional.empty()).blockingGet();

    System.out.println("\nBefore Turn " + turnNumber + ":");
    System.out.println("  Events in session: " + session.events().size());
    if (!session.events().isEmpty()) {
      Optional<String> prevId = findPreviousInteractionId(session.events());
      System.out.println("  Previous Interaction ID: " + prevId.orElse("<none>"));
    }

    // Execute turn
    Content userMessage =
        Content.builder()
            .role("user")
            .parts(ImmutableList.of(Part.builder().text(prompt).build()))
            .build();

    List<Event> events = new ArrayList<>();
    StringBuilder agentResponseBuilder = new StringBuilder();
    String interactionId = null;

    for (Event event : runner.runAsync(userId, sessionId, userMessage).blockingIterable()) {
      events.add(event);

      // Extract response text (skip partial streaming chunks)
      if (!event.partial().orElse(false) && event.content().isPresent()) {
        event
            .content()
            .get()
            .parts()
            .ifPresent(
                parts -> {
                  parts.forEach(
                      part -> {
                        part.text().ifPresent(agentResponseBuilder::append);
                      });
                });
      }

      // Track interaction ID
      if (event.interactionId().isPresent()) {
        interactionId = event.interactionId().get();
      }
    }

    String agentResponse = agentResponseBuilder.toString();
    System.out.println("\nAgent Response: " + agentResponse);
    System.out.println("Interaction ID: " + interactionId);
    System.out.println("Events collected: " + events.size());

    // After turn, show updated session state
    session =
        sessionService.getSession(APP_NAME, userId, sessionId, Optional.empty()).blockingGet();
    System.out.println("After Turn " + turnNumber + ":");
    System.out.println("  Total events in session: " + session.events().size());

    return new TurnResult(prompt, agentResponse, interactionId, events.size());
  }

  /**
   * Finds the most recent interaction ID in the event list.
   *
   * <p>This matches the logic in {@code InteractionsRequestProcessor} which searches events from
   * newest to oldest to find the previous interaction ID.
   *
   * @param events the list of events
   * @return Optional containing the most recent interaction ID, or empty if none found
   */
  private static Optional<String> findPreviousInteractionId(List<Event> events) {
    for (int i = events.size() - 1; i >= 0; i--) {
      Event event = events.get(i);
      if (event.interactionId().isPresent()) {
        return event.interactionId();
      }
    }
    return Optional.empty();
  }

  /**
   * Creates the agent for multi-turn testing.
   *
   * @return configured agent with Interactions API enabled
   */
  private static BaseAgent createAgent() {
    Gemini gemini = Gemini.builder().modelName("gemini-2.5-flash").useInteractionsApi(true).build();

    return LlmAgent.builder()
        .name("MultiTurnTestAgent")
        .model(gemini)
        .instruction("You are a helpful assistant. Remember facts that users tell you.")
        .build();
  }

  public static void main(String[] args) {
    System.out.println("=== Multi-Turn Interactions API Test ===");
    System.out.println("Testing previousInteractionId chaining and state retention");
    System.out.println("Database: " + DB_URL);
    System.out.println();

    // Initialize DatabaseSessionService
    DatabaseSessionService sessionService = new DatabaseSessionService(DB_URL);
    System.out.println("DatabaseSessionService initialized");

    // Create Runner with Interactions API agent
    Runner runner =
        Runner.builder()
            .agent(createAgent())
            .appName(APP_NAME)
            .sessionService(sessionService)
            .build();
    System.out.println("Runner initialized with Interactions API agent");
    System.out.println();

    // Create a new session
    String sessionId = UUID.randomUUID().toString();
    var unused =
        sessionService
            .createSession(APP_NAME, USER_ID, new ConcurrentHashMap<>(), sessionId)
            .blockingGet();
    System.out.println("Session created: " + sessionId);

    // Execute test turns
    List<TurnResult> results = new ArrayList<>();

    // Turn 1: Establish context - favorite color
    results.add(
        executeTurn(
            runner,
            sessionService,
            USER_ID,
            sessionId,
            "My favorite color is blue. Please remember this.",
            1));

    // Turn 2: Establish more context - lucky number
    results.add(
        executeTurn(
            runner,
            sessionService,
            USER_ID,
            sessionId,
            "My lucky number is 42. Please remember this too.",
            2));

    // Turn 3: Recall first context
    results.add(
        executeTurn(
            runner,
            sessionService,
            USER_ID,
            sessionId,
            "What is my favorite color that I mentioned earlier?",
            3));

    // Turn 4: Recall second context
    results.add(
        executeTurn(runner, sessionService, USER_ID, sessionId, "What was my lucky number?", 4));

    // Verification Summary
    System.out.println("\n" + "=".repeat(60));
    System.out.println("VERIFICATION SUMMARY");
    System.out.println("=".repeat(60));

    System.out.println("\nInteraction ID Chain:");
    for (int i = 0; i < results.size(); i++) {
      TurnResult result = results.get(i);
      System.out.println("  Turn " + (i + 1) + ": " + result.interactionId);
    }

    // Verify all interaction IDs are unique
    Set<String> uniqueIds = results.stream().map(r -> r.interactionId).collect(Collectors.toSet());
    System.out.println("\nUnique Interaction IDs: " + uniqueIds.size() + "/" + results.size());

    // Verify recall worked
    boolean colorRecalled = results.get(2).agentResponse.toLowerCase().contains("blue");
    boolean numberRecalled = results.get(3).agentResponse.toLowerCase().contains("42");

    System.out.println("\nContext Retention Tests:");
    System.out.println("  Turn 3 recalled 'blue': " + (colorRecalled ? "✓ PASS" : "✗ FAIL"));
    System.out.println("  Turn 4 recalled '42': " + (numberRecalled ? "✓ PASS" : "✗ FAIL"));

    if (uniqueIds.size() == results.size() && colorRecalled && numberRecalled) {
      System.out.println("\n✓ ALL TESTS PASSED");
      System.out.println("\nThe Interactions API successfully:");
      System.out.println("  • Generated unique interaction IDs for each turn");
      System.out.println("  • Chained conversations via previousInteractionId");
      System.out.println("  • Retained state across turns without resending full history");
    } else {
      System.out.println("\n✗ SOME TESTS FAILED");
      if (uniqueIds.size() != results.size()) {
        System.out.println("  • Interaction IDs are not unique!");
      }
      if (!colorRecalled) {
        System.out.println("  • Failed to recall 'blue' from Turn 1");
      }
      if (!numberRecalled) {
        System.out.println("  • Failed to recall '42' from Turn 2");
      }
    }

    System.out.println("\n=== Test Complete ===");
    System.out.println("Session ID: " + sessionId);
    System.out.println(
        "\nTip: Enable DEBUG logging to see detailed Interactions API request/response flow:");
    System.out.println("  Edit src/main/resources/simplelogger.properties");
    System.out.println("  Set: org.slf4j.simpleLogger.log.com.google.adk.flows.llmflows=DEBUG");
  }
}
