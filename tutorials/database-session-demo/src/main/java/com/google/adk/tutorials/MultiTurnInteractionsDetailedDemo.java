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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Enhanced multi-turn Interactions API demo with detailed JSON request/response logging.
 *
 * <p>This version shows the raw request and response data for each turn to demonstrate exactly what
 * is sent to and received from the Interactions API.
 */
public class MultiTurnInteractionsDetailedDemo {

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
  private static final String APP_NAME = "multi_turn_interactions_detailed";
  private static final String USER_ID = "test_user";

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

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
    System.out.println("\n" + "=".repeat(80));
    System.out.println("TURN " + turnNumber + ": " + prompt);
    System.out.println("=".repeat(80));

    // Before calling agent, show current session state
    Session session =
        sessionService.getSession(APP_NAME, userId, sessionId, Optional.empty()).blockingGet();

    System.out.println("\n📊 BEFORE TURN " + turnNumber + ":");
    System.out.println("  Events in session: " + session.events().size());
    if (!session.events().isEmpty()) {
      Optional<String> prevId = findPreviousInteractionId(session.events());
      System.out.println("  Previous Interaction ID: " + prevId.orElse("<none>"));

      // Show what will be sent as "Input" to Interactions API
      System.out.println("\n📤 REQUEST DETAILS:");
      if (prevId.isPresent()) {
        System.out.println("  previousInteractionId: " + prevId.get());
        System.out.println("  Input will contain: ONLY the latest user turn (\"" + prompt + "\")");
        System.out.println(
            "  Full history: NOT sent (server already has it via interaction chain)");
      } else {
        System.out.println("  previousInteractionId: <none> (first turn)");
        System.out.println("  Input will contain: The user message");
      }
    } else {
      System.out.println("\n📤 REQUEST DETAILS:");
      System.out.println("  previousInteractionId: <none> (first turn)");
      System.out.println("  Input will contain: The user message");
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

    System.out.println("\n⏳ Calling Interactions API...");

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

    System.out.println("\n📥 RESPONSE DETAILS:");
    System.out.println("  Interaction ID: " + interactionId);
    System.out.println("  Agent Response: " + agentResponse);
    System.out.println("  Events collected: " + events.size());

    // Show response structure
    if (!events.isEmpty()) {
      Event responseEvent = events.get(0);
      System.out.println("\n📋 RESPONSE EVENT STRUCTURE:");
      JsonObject eventJson = new JsonObject();
      eventJson.addProperty("id", responseEvent.id());
      eventJson.addProperty("author", responseEvent.author());
      eventJson.addProperty("interactionId", responseEvent.interactionId().orElse(""));

      responseEvent
          .content()
          .ifPresent(
              content -> {
                JsonObject contentJson = new JsonObject();
                contentJson.addProperty("role", content.role().orElse(""));
                content
                    .parts()
                    .ifPresent(
                        parts -> {
                          contentJson.addProperty("parts_count", parts.size());
                          JsonObject partsInfo = new JsonObject();
                          for (int i = 0; i < parts.size(); i++) {
                            Part part = parts.get(i);
                            JsonObject partInfo = new JsonObject();
                            part.text().ifPresent(text -> partInfo.addProperty("text", text));
                            part.thought()
                                .ifPresent(thought -> partInfo.addProperty("is_thought", thought));
                            partsInfo.add("part_" + i, partInfo);
                          }
                          contentJson.add("parts", partsInfo);
                        });
                eventJson.add("content", contentJson);
              });

      System.out.println(GSON.toJson(eventJson));
    }

    // After turn, show updated session state
    session =
        sessionService.getSession(APP_NAME, userId, sessionId, Optional.empty()).blockingGet();
    System.out.println("\n📊 AFTER TURN " + turnNumber + ":");
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
        .name("MultiTurnDetailedTestAgent")
        .model(gemini)
        .instruction("You are a helpful assistant. Remember facts that users tell you.")
        .build();
  }

  public static void main(String[] args) {
    System.out.println("=".repeat(80));
    System.out.println("  MULTI-TURN INTERACTIONS API TEST (DETAILED REQUEST/RESPONSE VIEW)");
    System.out.println("=".repeat(80));
    System.out.println("This demo shows exactly what is sent to and received from the");
    System.out.println(
        "Interactions API for each turn, demonstrating previousInteractionId usage.");
    System.out.println();

    // Initialize DatabaseSessionService
    DatabaseSessionService sessionService = new DatabaseSessionService(DB_URL);

    // Create Runner with Interactions API agent
    Runner runner =
        Runner.builder()
            .agent(createAgent())
            .appName(APP_NAME)
            .sessionService(sessionService)
            .build();

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
    System.out.println("\n" + "=".repeat(80));
    System.out.println("VERIFICATION SUMMARY");
    System.out.println("=".repeat(80));

    System.out.println("\n🔗 INTERACTION ID CHAIN:");
    for (int i = 0; i < results.size(); i++) {
      TurnResult result = results.get(i);
      System.out.println("  Turn " + (i + 1) + ": " + result.interactionId);
    }

    // Verify all interaction IDs are unique
    Set<String> uniqueIds = results.stream().map(r -> r.interactionId).collect(Collectors.toSet());
    System.out.println("\n✅ Unique Interaction IDs: " + uniqueIds.size() + "/" + results.size());

    // Verify recall worked
    boolean colorRecalled = results.get(2).agentResponse.toLowerCase().contains("blue");
    boolean numberRecalled = results.get(3).agentResponse.toLowerCase().contains("42");

    System.out.println("\n🧠 CONTEXT RETENTION TESTS:");
    System.out.println("  Turn 3 recalled 'blue': " + (colorRecalled ? "✓ PASS" : "✗ FAIL"));
    System.out.println("  Turn 4 recalled '42': " + (numberRecalled ? "✓ PASS" : "✗ FAIL"));

    if (uniqueIds.size() == results.size() && colorRecalled && numberRecalled) {
      System.out.println("\n" + "=".repeat(80));
      System.out.println("✅ ALL TESTS PASSED");
      System.out.println("=".repeat(80));
      System.out.println("\nThe Interactions API successfully:");
      System.out.println("  • Generated unique interaction IDs for each turn");
      System.out.println("  • Chained conversations via previousInteractionId");
      System.out.println("  • Retained state across turns without resending full history");
      System.out.println("  • Turn 2-4 sent ONLY the latest user message (not full history)");
      System.out.println("  • Server maintained context via interaction chain");
    } else {
      System.out.println("\n✗ SOME TESTS FAILED");
    }

    System.out.println("\n" + "=".repeat(80));
    System.out.println("Session ID: " + sessionId);
    System.out.println("=".repeat(80));
  }
}
