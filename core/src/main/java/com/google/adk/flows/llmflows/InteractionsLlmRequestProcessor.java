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
import com.google.adk.models.Gemini;
import com.google.adk.models.LlmRequest;
import com.google.common.collect.ImmutableList;
import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request processor that extracts the previous interaction ID from session events for stateful
 * conversation chaining via the Interactions API.
 *
 * <p>This processor should be placed before the Contents processor in the request processor chain
 * so that it can set the previousInteractionId before contents are processed.
 */
public final class InteractionsLlmRequestProcessor implements RequestProcessor {

  private static final Logger logger =
      LoggerFactory.getLogger(InteractionsLlmRequestProcessor.class);

  @Override
  public Single<RequestProcessingResult> processRequest(
      InvocationContext context, LlmRequest request) {

    // Only process if the agent uses a Gemini model with useInteractionsApi enabled
    if (!shouldUseInteractionsApi(context)) {
      return Single.just(RequestProcessingResult.create(request, ImmutableList.of()));
    }

    // Search for the most recent interaction ID in session events
    Optional<String> previousInteractionId =
        findPreviousInteractionId(context.session().events(), context.branch());

    if (previousInteractionId.isPresent()) {
      logger.debug("Found previous interaction ID: {}", previousInteractionId.get());
      LlmRequest updatedRequest =
          request.toBuilder().previousInteractionId(previousInteractionId.get()).build();
      return Single.just(RequestProcessingResult.create(updatedRequest, ImmutableList.of()));
    }

    return Single.just(RequestProcessingResult.create(request, ImmutableList.of()));
  }

  /**
   * Checks if the agent is configured to use the Interactions API.
   *
   * @param context The invocation context.
   * @return true if the agent uses a Gemini model with useInteractionsApi enabled.
   */
  private boolean shouldUseInteractionsApi(InvocationContext context) {
    if (!(context.agent() instanceof LlmAgent llmAgent)) {
      return false;
    }

    try {
      if (llmAgent.resolvedModel().model().isPresent()
          && llmAgent.resolvedModel().model().get() instanceof Gemini gemini) {
        return gemini.useInteractionsApi();
      }
    } catch (IllegalStateException e) {
      // Model not resolved yet
      return false;
    }

    return false;
  }

  /**
   * Searches session events in reverse order to find the most recent event with an interaction ID.
   *
   * @param events The list of session events.
   * @param currentBranch The current branch for filtering events in multi-agent scenarios.
   * @return The most recent interaction ID if found.
   */
  private Optional<String> findPreviousInteractionId(
      List<Event> events, Optional<String> currentBranch) {
    // Search in reverse order (most recent first)
    for (int i = events.size() - 1; i >= 0; i--) {
      Event event = events.get(i);

      // Filter by branch if applicable
      if (!isEventInBranch(event, currentBranch)) {
        continue;
      }

      // Check if this event has an interaction ID
      if (event.interactionId().isPresent() && !event.interactionId().get().isEmpty()) {
        return event.interactionId();
      }
    }

    return Optional.empty();
  }

  /**
   * Checks if an event belongs to the current branch.
   *
   * @param event The event to check.
   * @param currentBranch The current branch.
   * @return true if the event belongs to the current branch.
   */
  private boolean isEventInBranch(Event event, Optional<String> currentBranch) {
    // If no current branch specified, include all events
    if (currentBranch.isEmpty() || currentBranch.get().isEmpty()) {
      return true;
    }

    // If event has no branch, include it (root-level events)
    if (event.branch().isEmpty() || event.branch().get().isEmpty()) {
      return true;
    }

    // Check if current branch starts with the event's branch (hierarchical matching)
    return currentBranch.get().startsWith(event.branch().get());
  }
}
