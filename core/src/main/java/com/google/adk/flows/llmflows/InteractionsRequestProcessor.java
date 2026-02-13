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

package com.google.adk.flows.llmflows;

import com.google.adk.agents.InvocationContext;
import com.google.adk.agents.LlmAgent;
import com.google.adk.events.Event;
import com.google.adk.models.BaseLlm;
import com.google.adk.models.Gemini;
import com.google.adk.models.LlmRequest;
import com.google.common.collect.ImmutableList;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RequestProcessor} that sets up the previous interaction ID for the Interactions API.
 *
 * <p>When the Interactions API is enabled on the agent's Gemini model, this processor:
 *
 * <ul>
 *   <li>Walks the session events in reverse to find the most recent interaction ID
 *   <li>Sets the {@code previousInteractionId} on the LLM request
 * </ul>
 *
 * <p>This enables multi-turn conversation chaining via the Interactions API, where the server
 * maintains conversation state and only the latest user turn needs to be sent.
 */
public final class InteractionsRequestProcessor implements RequestProcessor {

  private static final Logger logger = LoggerFactory.getLogger(InteractionsRequestProcessor.class);

  @Override
  public Single<RequestProcessingResult> processRequest(
      InvocationContext context, LlmRequest request) {

    if (!isInteractionsApiEnabled(context)) {
      return Single.just(RequestProcessingResult.create(request, ImmutableList.of()));
    }

    // Find the previous interaction ID from session events
    Optional<String> previousInteractionId = findPreviousInteractionId(context.session().events());

    if (previousInteractionId.isPresent()) {
      logger.debug("Found previousInteractionId: {}", previousInteractionId.get());
      LlmRequest updatedRequest =
          request.toBuilder().previousInteractionId(previousInteractionId.get()).build();
      return Single.just(RequestProcessingResult.create(updatedRequest, ImmutableList.of()));
    }

    logger.debug("No previousInteractionId found in session events");
    return Single.just(RequestProcessingResult.create(request, ImmutableList.of()));
  }

  /** Checks whether the Interactions API is enabled for the current agent. */
  private static boolean isInteractionsApiEnabled(InvocationContext context) {
    if (!(context.agent() instanceof LlmAgent llmAgent)) {
      return false;
    }
    try {
      Optional<BaseLlm> modelOpt = llmAgent.resolvedModel().model();
      if (modelOpt.isPresent() && modelOpt.get() instanceof Gemini gemini) {
        return gemini.useInteractionsApi();
      }
    } catch (IllegalStateException e) {
      // Model not resolved yet
    }
    return false;
  }

  /**
   * Finds the most recent interaction ID from session events by walking backwards.
   *
   * @param events The list of session events.
   * @return The most recent interaction ID, or empty if none found.
   */
  static Optional<String> findPreviousInteractionId(List<Event> events) {
    for (int i = events.size() - 1; i >= 0; i--) {
      Event event = events.get(i);
      if (event.interactionId().isPresent()) {
        return event.interactionId();
      }
    }
    return Optional.empty();
  }
}
