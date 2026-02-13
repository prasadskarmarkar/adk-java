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

package com.google.adk.models;

import com.google.adk.tools.BaseTool;
import com.google.genai.Client;
import com.google.genai.InteractionEventStream;
import com.google.genai.types.FunctionDeclaration;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.interactions.CreateInteractionConfig;
import com.google.genai.types.interactions.GenerationConfig;
import com.google.genai.types.interactions.Input;
import com.google.genai.types.interactions.Interaction;
import com.google.genai.types.interactions.InteractionStatus;
import com.google.genai.types.interactions.Turn;
import com.google.genai.types.interactions.content.Content;
import com.google.genai.types.interactions.content.FunctionCallContent;
import com.google.genai.types.interactions.content.FunctionResultContent;
import com.google.genai.types.interactions.content.TextContent;
import com.google.genai.types.interactions.content.ThoughtContent;
import com.google.genai.types.interactions.content.ThoughtSummaryContent;
import com.google.genai.types.interactions.streaming.ContentDelta;
import com.google.genai.types.interactions.streaming.ContentStart;
import com.google.genai.types.interactions.streaming.ContentStop;
import com.google.genai.types.interactions.streaming.InteractionEvent;
import com.google.genai.types.interactions.streaming.InteractionSseEvent;
import com.google.genai.types.interactions.streaming.delta.Delta;
import com.google.genai.types.interactions.streaming.delta.FunctionCallDelta;
import com.google.genai.types.interactions.streaming.delta.TextDelta;
import com.google.genai.types.interactions.streaming.delta.ThoughtSignatureDelta;
import com.google.genai.types.interactions.streaming.delta.ThoughtSummaryDelta;
import com.google.genai.types.interactions.tools.CodeExecution;
import com.google.genai.types.interactions.tools.ComputerUse;
import com.google.genai.types.interactions.tools.Function;
import com.google.genai.types.interactions.tools.GoogleSearch;
import com.google.genai.types.interactions.tools.Tool;
import com.google.genai.types.interactions.tools.UrlContext;
import io.reactivex.rxjava3.core.Flowable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for integrating with the Gemini Interactions API.
 *
 * <p>Provides methods to convert ADK types to Interactions API types, and to call the Interactions
 * API for both streaming and non-streaming interactions.
 */
public final class InteractionsUtils {

  private static final Logger logger = LoggerFactory.getLogger(InteractionsUtils.class);

  private InteractionsUtils() {}

  /**
   * Generates content via the Interactions API instead of the standard GenerateContent API.
   *
   * @param client The genai Client instance.
   * @param llmRequest The LLM request containing contents, config, and tools.
   * @param stream Whether to use streaming mode.
   * @return A Flowable of LlmResponse objects.
   */
  public static Flowable<LlmResponse> generateContentViaInteractions(
      Client client, LlmRequest llmRequest, boolean stream) {

    CreateInteractionConfig.Builder configBuilder = CreateInteractionConfig.builder();

    // Set model
    String modelName =
        llmRequest
            .model()
            .orElseThrow(
                () -> new IllegalStateException("Model name must be set for Interactions API"));
    configBuilder.model(modelName);

    // When previousInteractionId is set, only send the latest user turn
    // since the server already has the conversation history.
    List<com.google.genai.types.Content> contents = llmRequest.contents();
    if (llmRequest.previousInteractionId().isPresent()) {
      contents = getLatestUserContents(contents);
    }
    Input input = convertContentsToInput(contents);
    configBuilder.input(input);

    // Set previousInteractionId if present
    llmRequest.previousInteractionId().ifPresent(configBuilder::previousInteractionId);

    // Extract and set system instruction
    extractSystemInstruction(llmRequest.config()).ifPresent(configBuilder::systemInstruction);

    // Build and set generation config
    buildGenerationConfig(llmRequest.config()).ifPresent(configBuilder::generationConfig);

    // Convert and set tools
    List<Tool> interactionsTools = convertToolsToInteractionsFormat(llmRequest);
    if (!interactionsTools.isEmpty()) {
      configBuilder.tools(interactionsTools);
    }

    CreateInteractionConfig config = configBuilder.build();

    logger.debug("=== INTERACTIONS API REQUEST ===");
    logger.debug("Model: {}", modelName);
    logger.debug(
        "Previous Interaction ID: {}", llmRequest.previousInteractionId().orElse("<none>"));
    logger.debug("Input: {}", input);
    logger.debug("System Instruction: {}", config.systemInstruction().orElse("<none>"));
    logger.debug("Generation Config: {}", config.generationConfig().orElse(null));
    logger.debug("Tools: {}", interactionsTools.isEmpty() ? "<none>" : interactionsTools);
    logger.debug("================================");

    if (stream) {
      return streamingCreate(client, config);
    } else {
      return nonStreamingCreate(client, config);
    }
  }

  /** Performs a non-streaming Interactions API call and converts the response to LlmResponse. */
  private static Flowable<LlmResponse> nonStreamingCreate(
      Client client, CreateInteractionConfig config) {
    CompletableFuture<Interaction> future = client.async.interactions.create(config);

    return Flowable.fromFuture(future)
        .map(
            interaction -> {
              logger.debug("=== INTERACTIONS API RESPONSE ===");
              logger.debug("Interaction ID: {}", interaction.id());
              logger.debug("Status: {}", interaction.status());
              logger.debug("Outputs: {}", interaction.outputs().orElse(List.of()));
              logger.debug("=================================");
              return convertInteractionToLlmResponse(interaction);
            });
  }

  /** Performs a streaming Interactions API call and converts events to LlmResponse objects. */
  private static Flowable<LlmResponse> streamingCreate(
      Client client, CreateInteractionConfig config) {
    CompletableFuture<InteractionEventStream<InteractionSseEvent>> streamFuture =
        client.async.interactions.createStream(config);

    return Flowable.fromFuture(streamFuture)
        .flatMap(stream -> Flowable.fromIterable(stream))
        .concatMap(
            event -> {
              Optional<LlmResponse> response = convertInteractionEventToLlmResponse(event);
              return response.map(Flowable::just).orElse(Flowable.empty());
            })
        .filter(
            llmResponse ->
                llmResponse
                    .content()
                    .flatMap(com.google.genai.types.Content::parts)
                    .map(
                        parts ->
                            !parts.isEmpty()
                                && parts.stream()
                                    .anyMatch(
                                        p ->
                                            p.functionCall().isPresent()
                                                || p.functionResponse().isPresent()
                                                || p.text().map(t -> !t.isBlank()).orElse(false)))
                    .orElse(
                        llmResponse.turnComplete().orElse(false)
                            || llmResponse.interactionId().isPresent()));
  }

  /** Converts a non-streaming Interaction response to an LlmResponse. */
  static LlmResponse convertInteractionToLlmResponse(Interaction interaction) {
    LlmResponse.Builder builder = LlmResponse.builder();
    builder.interactionId(interaction.id());

    List<Content> outputs = interaction.outputs().orElse(List.of());
    List<com.google.genai.types.Part> parts = new ArrayList<>();

    logger.debug("=== Converting Interactions API Response to ADK LlmResponse ===");
    logger.debug("Number of outputs: {}", outputs.size());

    for (Content output : outputs) {
      logger.debug("  Output type: {}", output.getClass().getSimpleName());
      Optional<com.google.genai.types.Part> partOpt = convertInteractionsContentToPart(output);
      if (partOpt.isPresent()) {
        logger.debug("  Converted to Part: {}", partOpt.get());
        parts.add(partOpt.get());
      } else {
        logger.debug("  Skipped (no Part conversion)");
      }
    }

    if (!parts.isEmpty()) {
      com.google.genai.types.Content content =
          com.google.genai.types.Content.builder().role("model").parts(parts).build();
      logger.debug("Built ADK Content with {} parts", parts.size());
      builder.content(content);
    }

    // Check if action is required (function calling)
    if (interaction.status().knownEnum() == InteractionStatus.Known.REQUIRES_ACTION) {
      // The response needs function results - this is normal in the agentic loop
      logger.debug("Interaction requires action (function calling)");
    }

    LlmResponse result = builder.build();
    logger.debug(
        "Final LlmResponse: interactionId={}, hasContent={}",
        result.interactionId().orElse("<none>"),
        result.content().isPresent());
    logger.debug("===============================================================");

    return result;
  }

  /**
   * Converts a streaming InteractionSseEvent to an LlmResponse.
   *
   * @return Optional.empty() if the event should be skipped, otherwise the LlmResponse.
   */
  static Optional<LlmResponse> convertInteractionEventToLlmResponse(InteractionSseEvent event) {
    if (event instanceof ContentDelta contentDelta) {
      return convertContentDeltaToLlmResponse(contentDelta);
    } else if (event instanceof ContentStart contentStart) {
      return convertContentStartToLlmResponse(contentStart);
    } else if (event instanceof ContentStop contentStop) {
      // ContentStop just signals end of a content item
      return Optional.empty();
    } else if (event instanceof InteractionEvent interactionEvent) {
      return convertInteractionEventToLlmResponse(interactionEvent);
    } else {
      logger.trace("Skipping unhandled event type: {}", event.getClass().getSimpleName());
      return Optional.empty();
    }
  }

  /** Converts a ContentDelta event to LlmResponse. */
  private static Optional<LlmResponse> convertContentDeltaToLlmResponse(ContentDelta contentDelta) {
    Optional<Delta> deltaOpt = contentDelta.delta();
    if (deltaOpt.isEmpty()) {
      return Optional.empty();
    }

    Delta delta = deltaOpt.get();

    if (delta instanceof TextDelta textDelta) {
      String text = textDelta.text().orElse("");
      if (text.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(
          LlmResponse.builder()
              .content(
                  com.google.genai.types.Content.builder()
                      .role("model")
                      .parts(com.google.genai.types.Part.fromText(text))
                      .build())
              .partial(true)
              .build());
    } else if (delta instanceof FunctionCallDelta fcDelta) {
      // Function call deltas accumulate name and arguments
      List<com.google.genai.types.Part> parts = new ArrayList<>();
      String name = fcDelta.name().orElse(null);
      Map<String, Object> args = fcDelta.arguments().orElse(null);
      String id = fcDelta.id().orElse(null);

      if (name != null || args != null) {
        com.google.genai.types.FunctionCall.Builder fcBuilder =
            com.google.genai.types.FunctionCall.builder();
        if (name != null) {
          fcBuilder.name(name);
        }
        if (args != null) {
          fcBuilder.args(args);
        }
        if (id != null) {
          fcBuilder.id(id);
        }
        parts.add(com.google.genai.types.Part.builder().functionCall(fcBuilder.build()).build());

        return Optional.of(
            LlmResponse.builder()
                .content(
                    com.google.genai.types.Content.builder().role("model").parts(parts).build())
                .partial(true)
                .build());
      }
      return Optional.empty();
    } else if (delta instanceof ThoughtSummaryDelta || delta instanceof ThoughtSignatureDelta) {
      // Thought deltas - emit as thought parts
      if (delta instanceof ThoughtSummaryDelta thoughtDelta) {
        // ThoughtSummaryDelta contains summary text
        return Optional.of(
            LlmResponse.builder()
                .content(
                    com.google.genai.types.Content.builder()
                        .role("model")
                        .parts(
                            com.google.genai.types.Part.fromText("").toBuilder()
                                .thought(true)
                                .build())
                        .build())
                .partial(true)
                .build());
      }
      return Optional.empty();
    }

    logger.trace("Skipping unhandled delta type: {}", delta.getClass().getSimpleName());
    return Optional.empty();
  }

  /** Converts a ContentStart event to LlmResponse. */
  private static Optional<LlmResponse> convertContentStartToLlmResponse(ContentStart contentStart) {
    Optional<Content> contentOpt = contentStart.content();
    if (contentOpt.isEmpty()) {
      return Optional.empty();
    }

    Content content = contentOpt.get();

    if (content instanceof FunctionCallContent fcContent) {
      // Function call start - emit function call metadata
      com.google.genai.types.FunctionCall.Builder fcBuilder =
          com.google.genai.types.FunctionCall.builder();
      fcContent.name().ifPresent(fcBuilder::name);
      fcContent.arguments().ifPresent(fcBuilder::args);
      fcBuilder.id(fcContent.id());

      return Optional.of(
          LlmResponse.builder()
              .content(
                  com.google.genai.types.Content.builder()
                      .role("model")
                      .parts(
                          com.google.genai.types.Part.builder()
                              .functionCall(fcBuilder.build())
                              .build())
                      .build())
              .partial(true)
              .build());
    } else if (content instanceof ThoughtContent) {
      // Thought start - will be followed by deltas
      return Optional.empty();
    }

    return Optional.empty();
  }

  /** Converts an InteractionEvent (start/complete) to LlmResponse. */
  private static Optional<LlmResponse> convertInteractionEventToLlmResponse(
      InteractionEvent interactionEvent) {
    if (interactionEvent.isComplete()) {
      Optional<Interaction> interactionOpt = interactionEvent.interaction();
      if (interactionOpt.isPresent()) {
        Interaction interaction = interactionOpt.get();
        LlmResponse response = convertInteractionToLlmResponse(interaction);
        // Don't set turnComplete if function calls are present (action required)
        boolean hasActionRequired =
            interaction.status().knownEnum() == InteractionStatus.Known.REQUIRES_ACTION;
        if (!hasActionRequired) {
          response = response.toBuilder().turnComplete(true).build();
        }
        return Optional.of(response);
      }
      // Complete event without interaction - just signal turn complete
      return Optional.of(LlmResponse.builder().turnComplete(true).build());
    } else if (interactionEvent.isStart()) {
      // Start event - extract interaction ID
      Optional<Interaction> interactionOpt = interactionEvent.interaction();
      if (interactionOpt.isPresent()) {
        return Optional.of(LlmResponse.builder().interactionId(interactionOpt.get().id()).build());
      }
    }
    return Optional.empty();
  }

  /** Converts an Interactions API Content object to an ADK Part. */
  static Optional<com.google.genai.types.Part> convertInteractionsContentToPart(Content content) {
    if (content instanceof TextContent textContent) {
      return textContent
          .text()
          .filter(text -> !text.isEmpty())
          .map(com.google.genai.types.Part::fromText);
    } else if (content instanceof FunctionCallContent fcContent) {
      com.google.genai.types.FunctionCall.Builder fcBuilder =
          com.google.genai.types.FunctionCall.builder();
      fcContent.name().ifPresent(fcBuilder::name);
      fcContent.arguments().ifPresent(fcBuilder::args);
      fcBuilder.id(fcContent.id());
      return Optional.of(
          com.google.genai.types.Part.builder().functionCall(fcBuilder.build()).build());
    } else if (content instanceof FunctionResultContent frContent) {
      com.google.genai.types.FunctionResponse.Builder frBuilder =
          com.google.genai.types.FunctionResponse.builder();
      frContent.name().ifPresent(frBuilder::name);
      frBuilder.id(frContent.id());
      Object result = frContent.result();
      if (result instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> resultMap = (Map<String, Object>) result;
        frBuilder.response(resultMap);
      } else if (result instanceof String) {
        frBuilder.response(Map.of("result", result));
      }
      return Optional.of(
          com.google.genai.types.Part.builder().functionResponse(frBuilder.build()).build());
    } else if (content instanceof ThoughtContent thoughtContent) {
      // Convert thought to a Part with thought=true
      StringBuilder thoughtText = new StringBuilder();
      thoughtContent
          .summary()
          .ifPresent(
              summaries -> {
                for (ThoughtSummaryContent summary : summaries) {
                  if (summary instanceof TextContent tc) {
                    tc.text().ifPresent(thoughtText::append);
                  }
                }
              });
      String text = thoughtText.length() > 0 ? thoughtText.toString() : "";
      com.google.genai.types.Part.Builder partBuilder =
          com.google.genai.types.Part.fromText(text).toBuilder().thought(true);
      thoughtContent
          .signature()
          .ifPresent(sig -> partBuilder.thoughtSignature(Base64.getDecoder().decode(sig)));
      return Optional.of(partBuilder.build());
    }
    // Skip other content types (GoogleSearchCallContent, etc.) - not directly mappable to Parts
    logger.trace("Skipping unhandled content type: {}", content.getClass().getSimpleName());
    return Optional.empty();
  }

  /**
   * Converts ADK Content list to Interactions API Input.
   *
   * <p>Maps the ADK content format (with roles "user" and "model") to Interactions API Turn
   * objects.
   */
  static Input convertContentsToInput(List<com.google.genai.types.Content> contents) {
    if (contents.isEmpty()) {
      return Input.fromString("");
    }

    // If there's only one content with role "user" and just text, use simple input
    if (contents.size() == 1) {
      com.google.genai.types.Content content = contents.get(0);
      String role = content.role().orElse("user");
      if ("user".equals(role)) {
        Optional<String> text =
            content
                .parts()
                .flatMap(parts -> parts.stream().findFirst())
                .flatMap(com.google.genai.types.Part::text);
        if (text.isPresent() && content.parts().map(List::size).orElse(0) == 1) {
          return Input.fromString(text.get());
        }
      }
    }

    // Convert to Turns for multi-turn conversations
    List<Turn> turns = new ArrayList<>();
    for (com.google.genai.types.Content content : contents) {
      String role = content.role().orElse("user");
      List<Content> interactionsContents = convertAdkContentToInteractionsContents(content);

      if (!interactionsContents.isEmpty()) {
        Turn turn = Turn.builder().role(role).content(interactionsContents).build();
        turns.add(turn);
      }
    }

    if (turns.isEmpty()) {
      return Input.fromString("");
    }
    return Input.fromTurns(turns);
  }

  /** Converts an ADK Content object to a list of Interactions API Content objects. */
  private static List<Content> convertAdkContentToInteractionsContents(
      com.google.genai.types.Content adkContent) {
    List<Content> result = new ArrayList<>();
    List<com.google.genai.types.Part> parts = adkContent.parts().orElse(List.of());

    for (com.google.genai.types.Part part : parts) {
      if (part.text().isPresent() && !part.text().get().isEmpty()) {
        if (part.thought().orElse(false)) {
          // Skip thought parts in input - they're model-only
          continue;
        }
        result.add(TextContent.of(part.text().get()));
      } else if (part.functionCall().isPresent()) {
        com.google.genai.types.FunctionCall fc = part.functionCall().get();
        FunctionCallContent.Builder builder = FunctionCallContent.builder().id(fc.id().orElse(""));
        fc.name().ifPresent(builder::name);
        fc.args().ifPresent(builder::arguments);
        result.add(builder.build());
      } else if (part.functionResponse().isPresent()) {
        com.google.genai.types.FunctionResponse fr = part.functionResponse().get();
        FunctionResultContent.Builder builder =
            FunctionResultContent.builder().id(fr.id().orElse(""));
        fr.name().ifPresent(builder::name);
        Object resultObj = fr.response().map(r -> (Object) r).orElse(Map.of());
        builder.result(resultObj);
        result.add(builder.build());
      }
    }

    return result;
  }

  /**
   * Extracts the system instruction from a GenerateContentConfig.
   *
   * @return The system instruction text if present.
   */
  static Optional<String> extractSystemInstruction(Optional<GenerateContentConfig> configOpt) {
    return configOpt
        .flatMap(GenerateContentConfig::systemInstruction)
        .flatMap(
            si ->
                si.parts()
                    .flatMap(
                        parts ->
                            parts.stream()
                                .map(com.google.genai.types.Part::text)
                                .flatMap(Optional::stream)
                                .reduce((a, b) -> a + "\n" + b)));
  }

  /** Builds an Interactions API GenerationConfig from a GenerateContentConfig. */
  static Optional<GenerationConfig> buildGenerationConfig(
      Optional<GenerateContentConfig> configOpt) {
    if (configOpt.isEmpty()) {
      return Optional.empty();
    }

    GenerateContentConfig config = configOpt.get();
    GenerationConfig.Builder builder = GenerationConfig.builder();
    boolean hasAnyField = false;

    if (config.temperature().isPresent()) {
      builder.temperature(config.temperature().get());
      hasAnyField = true;
    }
    if (config.topP().isPresent()) {
      builder.topP(config.topP().get());
      hasAnyField = true;
    }
    if (config.seed().isPresent()) {
      builder.seed(config.seed().get());
      hasAnyField = true;
    }
    if (config.stopSequences().isPresent()) {
      builder.stopSequences(config.stopSequences().get());
      hasAnyField = true;
    }
    if (config.maxOutputTokens().isPresent()) {
      builder.maxOutputTokens(config.maxOutputTokens().get());
      hasAnyField = true;
    }

    return hasAnyField ? Optional.of(builder.build()) : Optional.empty();
  }

  /**
   * Converts ADK tools to Interactions API tool format.
   *
   * <p>Maps ADK tool types to their Interactions API equivalents:
   *
   * <ul>
   *   <li>FunctionTool (with FunctionDeclaration) -> interactions.tools.Function
   *   <li>GoogleSearchTool -> interactions.tools.GoogleSearch
   *   <li>CodeExecution (via config) -> interactions.tools.CodeExecution
   *   <li>UrlContext (via config) -> interactions.tools.UrlContext
   * </ul>
   */
  static List<Tool> convertToolsToInteractionsFormat(LlmRequest llmRequest) {
    List<Tool> result = new ArrayList<>();

    // Convert tools from the LlmRequest tools map (function tools)
    for (BaseTool tool : llmRequest.tools().values()) {
      Optional<FunctionDeclaration> declOpt = tool.declaration();
      if (declOpt.isPresent()) {
        FunctionDeclaration decl = declOpt.get();
        Function.Builder funcBuilder = Function.builder();
        decl.name().ifPresent(funcBuilder::name);
        decl.description().ifPresent(funcBuilder::description);
        decl.parameters().ifPresent(funcBuilder::parameters);
        result.add(funcBuilder.build());
      } else if (tool instanceof com.google.adk.tools.GoogleSearchTool) {
        result.add(GoogleSearch.builder().build());
      }
    }

    // Check GenerateContentConfig for additional tools (CodeExecution, GoogleSearch, UrlContext)
    llmRequest
        .config()
        .flatMap(GenerateContentConfig::tools)
        .ifPresent(
            configTools -> {
              for (com.google.genai.types.Tool configTool : configTools) {
                if (configTool.googleSearch().isPresent()) {
                  // Only add if not already added from tools map
                  if (result.stream().noneMatch(t -> t instanceof GoogleSearch)) {
                    result.add(GoogleSearch.builder().build());
                  }
                }
                if (configTool.codeExecution().isPresent()) {
                  result.add(CodeExecution.builder().build());
                }
                if (configTool.urlContext().isPresent()) {
                  result.add(UrlContext.builder().build());
                }
                if (configTool.computerUse().isPresent()) {
                  result.add(ComputerUse.builder().build());
                }
                // Also convert any function declarations from config tools
                configTool
                    .functionDeclarations()
                    .ifPresent(
                        declarations -> {
                          for (FunctionDeclaration decl : declarations) {
                            String declName = decl.name().orElse("");
                            // Avoid duplicates
                            boolean alreadyAdded =
                                result.stream()
                                    .filter(t -> t instanceof Function)
                                    .map(t -> (Function) t)
                                    .anyMatch(f -> f.name().orElse("").equals(declName));
                            if (!alreadyAdded) {
                              Function.Builder funcBuilder = Function.builder();
                              decl.name().ifPresent(funcBuilder::name);
                              decl.description().ifPresent(funcBuilder::description);
                              decl.parameters().ifPresent(funcBuilder::parameters);
                              result.add(funcBuilder.build());
                            }
                          }
                        });
              }
            });

    return result;
  }

  /**
   * Extracts only the latest user turn contents from the full content list.
   *
   * <p>When previousInteractionId is set, the server already has the conversation history, so only
   * the latest user turn needs to be sent.
   */
  static List<com.google.genai.types.Content> getLatestUserContents(
      List<com.google.genai.types.Content> contents) {
    if (contents.isEmpty()) {
      return contents;
    }

    // Walk backwards to find the latest user content
    for (int i = contents.size() - 1; i >= 0; i--) {
      com.google.genai.types.Content content = contents.get(i);
      String role = content.role().orElse("");
      if ("user".equals(role)) {
        return contents.subList(i, contents.size());
      }
    }

    // If no user content found, return all contents
    return contents;
  }
}
