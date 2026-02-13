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
import com.google.adk.events.Event;
import com.google.adk.models.Gemini;
import com.google.adk.runner.Runner;
import com.google.adk.sessions.DatabaseSessionService;
import com.google.adk.sessions.Session;
import com.google.adk.sessions.State;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Interactive chatbot tutorial demonstrating: - How to build a conversation loop with
 * DatabaseSessionService - Detecting and saving user preferences to user state - Using state across
 * conversation turns - Special commands for inspecting state
 *
 * <p>The chatbot can:
 *
 * <ul>
 *   <li>Remember user preferences (favorite color, name, dietary restrictions, etc.)
 *   <li>Use preferences to personalize responses
 *   <li>Track conversation metadata (message count, topic)
 *   <li>Respond to commands like /state, /preferences, /reset
 * </ul>
 */
public class InteractiveChatbotDemo {

  private static final String APP_NAME = "chatbot_demo";
  private static final String USER_ID = "demo_user";
  private static final String DB_URL = "jdbc:h2:./chatbot_demo_db;MODE=PostgreSQL";

  private static final Gson GSON = new Gson();

  public static void main(String[] args) {
    System.out.println("=== Interactive Chatbot with State Management ===");
    System.out.println("This chatbot remembers your preferences across sessions!");
    System.out.println();
    System.out.println("Try saying things like:");
    System.out.println("  - My favorite color is blue");
    System.out.println("  - I like pizza");
    System.out.println("  - Call me Alex");
    System.out.println("  - What's my favorite color?");
    System.out.println();
    System.out.println("Special commands:");
    System.out.println("  /state       - Show all state (app, user, session)");
    System.out.println("  /preferences - Show just your preferences");
    System.out.println("  /reset       - Clear your preferences");
    System.out.println("  /help        - Show this help message");
    System.out.println("  exit/quit    - Exit the chatbot");
    System.out.println();
    System.out.println("-------------------------------------------------\n");

    try (DatabaseSessionService sessionService = new DatabaseSessionService(DB_URL);
        Scanner scanner = new Scanner(System.in)) {

      // Generate a new session ID for this conversation
      String sessionId = UUID.randomUUID().toString();

      // Create the agent
      Gemini model = Gemini.builder().modelName("gemini-2.0-flash-exp").build();
      BaseAgent preferenceAgent = PreferenceAgent.create(model);

      // Create the runner
      Runner runner =
          Runner.builder()
              .agent(preferenceAgent)
              .sessionService(sessionService)
              .appName(APP_NAME)
              .build();

      // Initialize session with starting state
      ConcurrentHashMap<String, Object> initialState = new ConcurrentHashMap<>();
      initialState.put("message_count", 0);
      initialState.put("session_started", System.currentTimeMillis());

      sessionService.createSession(APP_NAME, USER_ID, initialState, sessionId).blockingGet();
      System.out.println("Session created: " + sessionId.substring(0, 8) + "...");
      System.out.println("Ready to chat!\n");

      // Main conversation loop
      while (true) {
        System.out.print("You: ");
        String userInput = scanner.nextLine().trim();

        if (userInput.isEmpty()) {
          continue;
        }

        // Check for exit commands
        if (userInput.equalsIgnoreCase("exit") || userInput.equalsIgnoreCase("quit")) {
          System.out.println("\nGoodbye! Your preferences have been saved.");
          break;
        }

        // Handle special commands
        if (userInput.startsWith("/")) {
          handleCommand(userInput, sessionService, sessionId);
          continue;
        }

        // Process the user message through the agent
        Content userMessage =
            Content.builder()
                .role("user")
                .parts(ImmutableList.of(Part.builder().text(userInput).build()))
                .build();

        try {
          List<Event> events =
              Lists.newArrayList(
                  runner.runAsync(USER_ID, sessionId, userMessage).blockingIterable());

          // Process events
          boolean preferenceDetected = false;
          for (Event event : events) {
            // Check for preference detection
            if (event.author().equals("PreferenceDetector")) {
              Object detectionObj = event.actions().stateDelta().get("preference_detection");
              if (detectionObj != null) {
                preferenceDetected =
                    handlePreferenceDetection(detectionObj.toString(), sessionService, sessionId);
              }
            }

            // Display conversation response
            if (event.author().equals("ConversationAgent")) {
              event
                  .content()
                  .ifPresent(
                      content ->
                          content
                              .parts()
                              .ifPresent(
                                  parts ->
                                      parts.forEach(
                                          part -> {
                                            if (part.text() != null) {
                                              System.out.println("Bot: " + part.text());
                                            }
                                          })));
            }
          }

          // Increment message count
          incrementMessageCount(sessionService, sessionId);

          // Acknowledge preference if detected
          if (preferenceDetected) {
            System.out.println("(Preference saved to your profile)");
          }

          System.out.println();

        } catch (Exception e) {
          System.err.println("Error processing message: " + e.getMessage());
          e.printStackTrace();
        }
      }

    } catch (Exception e) {
      System.err.println("Error initializing chatbot: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /** Handles special commands like /state, /preferences, /reset, /help. */
  private static void handleCommand(
      String command, DatabaseSessionService sessionService, String sessionId) {
    switch (command.toLowerCase()) {
      case "/help":
        System.out.println("\nAvailable commands:");
        System.out.println("  /state       - Show all state (app, user, session)");
        System.out.println("  /preferences - Show just your preferences");
        System.out.println("  /reset       - Clear your preferences");
        System.out.println("  /help        - Show this help message");
        System.out.println("  exit/quit    - Exit the chatbot\n");
        break;

      case "/state":
        displayAllState(sessionService, sessionId);
        break;

      case "/preferences":
        displayPreferences(sessionService);
        break;

      case "/reset":
        resetPreferences(sessionService);
        break;

      default:
        System.out.println("Unknown command: " + command);
        System.out.println("Type /help for available commands\n");
    }
  }

  /** Parses preference detection JSON and saves to user state if valid. */
  private static boolean handlePreferenceDetection(
      String detectionJson, DatabaseSessionService sessionService, String sessionId) {
    try {
      JsonObject detection = GSON.fromJson(detectionJson, JsonObject.class);

      if (detection.has("is_preference") && detection.get("is_preference").getAsBoolean()) {
        String key = detection.get("key").getAsString();
        String value = detection.get("value").getAsString();

        // Save to user state
        Map<String, Object> userState =
            sessionService.getUserState(APP_NAME, USER_ID).blockingGet();
        if (userState == null) {
          userState = new HashMap<>();
        }
        userState.put(key, value);
        sessionService.setUserState(APP_NAME, USER_ID, userState).blockingAwait();

        return true;
      }
    } catch (Exception e) {
      // Failed to parse - not a valid preference detection
      System.err.println("Failed to parse preference: " + e.getMessage());
    }
    return false;
  }

  /** Displays all state (app, user, session) for debugging. */
  private static void displayAllState(DatabaseSessionService sessionService, String sessionId) {
    try {
      Session session =
          sessionService.getSession(APP_NAME, USER_ID, sessionId, Optional.empty()).blockingGet();

      System.out.println("\n=== Current State ===");

      System.out.println("\nApp State (shared across all users):");
      session.state().entrySet().stream()
          .filter(e -> e.getKey().startsWith(State.APP_PREFIX))
          .forEach(e -> System.out.println("  " + e.getKey() + " = " + e.getValue()));

      System.out.println("\nUser State (your preferences):");
      session.state().entrySet().stream()
          .filter(e -> e.getKey().startsWith(State.USER_PREFIX))
          .forEach(e -> System.out.println("  " + e.getKey() + " = " + e.getValue()));

      System.out.println("\nSession State (this conversation):");
      session.state().entrySet().stream()
          .filter(
              e ->
                  !e.getKey().startsWith(State.APP_PREFIX)
                      && !e.getKey().startsWith(State.USER_PREFIX)
                      && !e.getKey().startsWith(State.TEMP_PREFIX))
          .forEach(e -> System.out.println("  " + e.getKey() + " = " + e.getValue()));

      System.out.println();
    } catch (Exception e) {
      System.err.println("Error retrieving state: " + e.getMessage());
    }
  }

  /** Displays just the user preferences. */
  private static void displayPreferences(DatabaseSessionService sessionService) {
    try {
      Map<String, Object> userState = sessionService.getUserState(APP_NAME, USER_ID).blockingGet();

      System.out.println("\n=== Your Preferences ===");
      if (userState == null || userState.isEmpty()) {
        System.out.println("No preferences set yet.");
      } else {
        userState.forEach((key, value) -> System.out.println("  " + key + " = " + value));
      }
      System.out.println();
    } catch (Exception e) {
      System.err.println("Error retrieving preferences: " + e.getMessage());
    }
  }

  /** Resets (clears) all user preferences. */
  private static void resetPreferences(DatabaseSessionService sessionService) {
    try {
      sessionService.setUserState(APP_NAME, USER_ID, new HashMap<>()).blockingAwait();
      System.out.println("\nAll preferences cleared.\n");
    } catch (Exception e) {
      System.err.println("Error resetting preferences: " + e.getMessage());
    }
  }

  /** Increments the message count in session state. */
  private static void incrementMessageCount(
      DatabaseSessionService sessionService, String sessionId) {
    try {
      Session session =
          sessionService.getSession(APP_NAME, USER_ID, sessionId, Optional.empty()).blockingGet();
      Integer currentCount = (Integer) session.state().getOrDefault("message_count", 0);

      // Note: In a real implementation, you'd use event state deltas to update this
      // For simplicity in this demo, we're directly updating session state
      ConcurrentHashMap<String, Object> updatedState = new ConcurrentHashMap<>(session.state());
      updatedState.put("message_count", currentCount + 1);

      // This is a simplified update - in production you'd use appendEvent with state delta
      sessionService.createSession(APP_NAME, USER_ID, updatedState, sessionId).blockingGet();
    } catch (Exception e) {
      // Silently fail - message count is not critical
    }
  }
}
