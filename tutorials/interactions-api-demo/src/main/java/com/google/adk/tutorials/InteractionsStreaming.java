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
 * Streaming tutorial using the Interactions API.
 *
 * <p>Demonstrates streaming (SSE) mode with the Interactions API. Partial response chunks are
 * emitted as they arrive, and the final event carries the {@code interactionId} for chaining
 * subsequent turns.
 *
 * <p>Run with:
 *
 * <pre>
 * mvn exec:java@streaming -pl tutorials/interactions-api-demo
 * </pre>
 */
public class InteractionsStreaming {

  private static final String APP_NAME = "interactions_streaming";
  private static final String USER_ID = "user";

  private static BaseAgent createAgent() {
    return LlmAgent.builder()
        .name("streaming_agent")
        .model(Gemini.builder().modelName("gemini-2.5-flash").useInteractionsApi(true).build())
        .description("Agent that streams responses via the Interactions API.")
        .instruction(
            "You are a helpful assistant. Provide detailed answers to demonstrate streaming.")
        .outputKey("streaming_output")
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

    String prompt = "Explain how photosynthesis works in a few sentences.";
    System.out.println("User: " + prompt);
    System.out.print("Agent: ");

    Content userMessage =
        Content.builder()
            .role("user")
            .parts(ImmutableList.of(Part.builder().text(prompt).build()))
            .build();

    for (Event event : runner.runAsync(USER_ID, sessionId, userMessage).blockingIterable()) {
      if (event.content().isPresent()) {
        event
            .content()
            .get()
            .parts()
            .ifPresent(
                parts ->
                    parts.forEach(
                        part ->
                            part.text().filter(t -> !t.isEmpty()).ifPresent(System.out::print)));
      }
    }
    System.out.println();
  }
}
