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

import com.google.adk.events.Event;
import com.google.adk.runner.Runner;
import com.google.adk.sessions.DatabaseSessionService;
import com.google.common.collect.ImmutableList;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tutorial demonstrating DatabaseSessionService with the Interactions API.
 *
 * <p>This tutorial shows how to:
 *
 * <ul>
 *   <li>Configure DatabaseSessionService with PostgreSQL
 *   <li>Create an agent using the Gemini Interactions API ({@code useInteractionsApi=true})
 *   <li>Run multi-turn conversations with stateful interaction chaining
 *   <li>Observe {@code interactionId} propagation across turns
 * </ul>
 *
 * <p>The Interactions API chains conversations via {@code previousInteractionId}, so the server
 * maintains state and only the latest user turn is sent on subsequent calls.
 *
 * <p>Prerequisites:
 *
 * <ul>
 *   <li>PostgreSQL running on localhost:5432
 *   <li>Database created (will auto-create schema via Flyway migrations)
 *   <li>Environment variables set (or use defaults):
 *       <ul>
 *         <li>DB_HOST (default: localhost)
 *         <li>DB_PORT (default: 5432)
 *         <li>DB_NAME (default: adk_test)
 *         <li>DB_USER (default: adk_user)
 *         <li>DB_PASSWORD (default: adk_password)
 *       </ul>
 * </ul>
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java -Dexec.mainClass=com.google.adk.tutorials.DatabaseSessionDemo
 * </pre>
 */
public class DatabaseSessionDemo {

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
  private static final String APP_NAME = "database_session_demo";
  private static final String USER_ID = "demo_user";

  public static void main(String[] args) {
    System.out.println("=== DatabaseSessionService + Interactions API Tutorial ===");
    System.out.println("Database: " + DB_URL);
    System.out.println("App Name: " + APP_NAME);
    System.out.println("User ID: " + USER_ID);
    System.out.println();

    // Initialize DatabaseSessionService
    DatabaseSessionService sessionService = new DatabaseSessionService(DB_URL);
    System.out.println("DatabaseSessionService initialized");

    // Create Runner with the Interactions API agent and session service
    Runner runner =
        Runner.builder()
            .agent(SimpleQuestionAnswerAgent.ROOT_AGENT)
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
    System.out.println();

    // Test prompts - multi-turn to demonstrate interaction chaining
    String[] prompts = {"What is the capital of France?", "Tell me a fun fact about that city"};

    // Run each prompt
    for (int i = 0; i < prompts.length; i++) {
      String prompt = prompts[i];
      System.out.println("Prompt " + (i + 1) + ": " + prompt);
      System.out.println("------------------------------------------------------------");

      List<Event> events = new ArrayList<>();
      Content userMessage =
          Content.builder()
              .role("user")
              .parts(ImmutableList.of(Part.builder().text(prompt).build()))
              .build();
      for (Event event : runner.runAsync(USER_ID, sessionId, userMessage).blockingIterable()) {
        events.add(event);

        // Print agent responses (skip partial streaming chunks)
        if (!event.partial().orElse(false)) {
          event
              .content()
              .ifPresent(
                  content -> {
                    content
                        .parts()
                        .ifPresent(
                            parts -> {
                              parts.forEach(
                                  part -> {
                                    part.text()
                                        .filter(t -> !t.isEmpty())
                                        .ifPresent(t -> System.out.println("Agent: " + t));
                                  });
                            });
                  });

          // Show interaction ID to demonstrate chaining across turns
          event.interactionId().ifPresent(id -> System.out.println("[interactionId: " + id + "]"));
        }
      }

      System.out.println("Events collected: " + events.size());
      System.out.println();
    }

    System.out.println("=== Tutorial Complete ===");
    System.out.println("Session ID: " + sessionId);
    System.out.println("All conversation data stored in database: " + DB_NAME);
    System.out.println();
    System.out.println("To export database to CSV, run:");
    System.out.println("  mvn exec:java@export-db");
    System.out.println();
  }
}
