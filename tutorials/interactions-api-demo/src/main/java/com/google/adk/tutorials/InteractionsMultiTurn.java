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
import com.google.adk.sessions.InMemorySessionService;
import com.google.common.collect.ImmutableList;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-turn conversation tutorial using the Interactions API.
 *
 * <p>Demonstrates how the Interactions API chains turns via {@code previousInteractionId}. Each
 * turn only sends the new user message; the server retains the full conversation history. This
 * tutorial sends several turns and verifies that the agent recalls information from earlier in the
 * conversation.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java@multi-turn -pl tutorials/interactions-api-demo
 * </pre>
 */
public class InteractionsMultiTurn {

  private static final String APP_NAME = "interactions_multi_turn";
  private static final String USER_ID = "user";

  private static BaseAgent createAgent() {
    return LlmAgent.builder()
        .name("multi_turn_agent")
        .model(Gemini.builder().modelName("gemini-2.5-flash").useInteractionsApi(true).build())
        .description("Agent for multi-turn conversations via the Interactions API.")
        .instruction("You are a helpful assistant. Remember what the user tells you.")
        .build();
  }

  public static void main(String[] args) {
    InMemorySessionService sessionService = new InMemorySessionService();
    Runner runner =
        Runner.builder()
            .agent(createAgent())
            .appName(APP_NAME)
            .sessionService(sessionService)
            .build();

    String sessionId = UUID.randomUUID().toString();
    sessionService
        .createSession(APP_NAME, USER_ID, new ConcurrentHashMap<>(), sessionId)
        .blockingGet();

    String[] prompts = {
      "My favorite color is blue. Please remember this.",
      "My lucky number is 42. Please remember this too.",
      "What is my favorite color?",
      "What is my lucky number?"
    };

    for (String prompt : prompts) {
      System.out.println("\nUser: " + prompt);

      Content userMessage =
          Content.builder()
              .role("user")
              .parts(ImmutableList.of(Part.builder().text(prompt).build()))
              .build();

      for (Event event : runner.runAsync(USER_ID, sessionId, userMessage).blockingIterable()) {
        if (!event.partial().orElse(false) && event.content().isPresent()) {
          event
              .content()
              .get()
              .parts()
              .ifPresent(
                  parts ->
                      parts.forEach(
                          part ->
                              part.text()
                                  .filter(t -> !t.isEmpty())
                                  .ifPresent(text -> System.out.println("Agent: " + text))));
        }
      }
    }
  }
}
