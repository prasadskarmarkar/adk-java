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
import com.google.adk.tools.BuiltInCodeExecutionTool;
import com.google.adk.tools.FunctionTool;
import com.google.adk.tools.GoogleSearchTool;
import com.google.genai.types.Content;
import com.google.genai.types.FinishReason;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionDeclaration;
import com.google.genai.types.FunctionResponse;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponseUsageMetadata;
import com.google.genai.types.Part;
import com.google.genai.types.interactions.CreateInteractionConfig;
import com.google.genai.types.interactions.GenerationConfig;
import com.google.genai.types.interactions.Input;
import com.google.genai.types.interactions.Interaction;
import com.google.genai.types.interactions.InteractionStatus;
import com.google.genai.types.interactions.Turn;
import com.google.genai.types.interactions.content.FunctionCallContent;
import com.google.genai.types.interactions.content.FunctionResultContent;
import com.google.genai.types.interactions.content.TextContent;
import com.google.genai.types.interactions.streaming.ContentDelta;
import com.google.genai.types.interactions.streaming.ContentStart;
import com.google.genai.types.interactions.streaming.InteractionEvent;
import com.google.genai.types.interactions.streaming.InteractionStreamingEvent;
import com.google.genai.types.interactions.streaming.delta.Delta;
import com.google.genai.types.interactions.streaming.delta.FunctionCallDelta;
import com.google.genai.types.interactions.streaming.delta.TextDelta;
import com.google.genai.types.interactions.tools.CodeExecution;
import com.google.genai.types.interactions.tools.Function;
import com.google.genai.types.interactions.tools.GoogleSearch;
import com.google.genai.types.interactions.tools.Tool;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for converting between ADK types and Interactions API types. */
public final class InteractionsUtils {

  private static final Logger logger = LoggerFactory.getLogger(InteractionsUtils.class);

  private InteractionsUtils() {}

  /**
   * Converts ADK Contents (List<Content>) to Interactions Turn list.
   *
   * @param contents The ADK contents to convert.
   * @return A list of Interactions Turn objects.
   */
  public static List<Turn> contentsToTurns(List<Content> contents) {
    List<Turn> turns = new ArrayList<>();
    for (Content content : contents) {
      List<com.google.genai.types.interactions.content.Content> turnContents = new ArrayList<>();
      if (content.parts().isPresent()) {
        for (Part part : content.parts().get()) {
          partToInteractionsContent(part).ifPresent(turnContents::add);
        }
      }
      if (!turnContents.isEmpty()) {
        Turn.Builder turnBuilder = Turn.builder().content(turnContents);
        content.role().ifPresent(turnBuilder::role);
        turns.add(turnBuilder.build());
      }
    }
    return turns;
  }

  /**
   * Converts an ADK Part to Interactions Content.
   *
   * @param part The ADK Part to convert.
   * @return An Optional containing the Interactions Content, or empty if not convertible.
   */
  public static Optional<com.google.genai.types.interactions.content.Content>
      partToInteractionsContent(Part part) {
    // Text content
    if (part.text().isPresent() && !part.text().get().isEmpty()) {
      return Optional.of(TextContent.of(part.text().get()));
    }

    // Function call
    if (part.functionCall().isPresent()) {
      FunctionCall fc = part.functionCall().get();
      String id = fc.id().orElse("");
      String name = fc.name().orElse("");
      Map<String, Object> args = fc.args().orElse(new HashMap<>());
      return Optional.of(FunctionCallContent.of(id, name, args));
    }

    // Function response
    if (part.functionResponse().isPresent()) {
      FunctionResponse fr = part.functionResponse().get();
      String id = fr.id().orElse("");
      String name = fr.name().orElse("");
      Map<String, Object> response = fr.response().orElse(new HashMap<>());
      return Optional.of(FunctionResultContent.of(id, name, response));
    }

    return Optional.empty();
  }

  /**
   * Converts ADK tools map to Interactions Tool list.
   *
   * @param tools The ADK tools map to convert.
   * @return A list of Interactions Tool objects.
   */
  public static List<Tool> toInteractionsTools(Map<String, BaseTool> tools) {
    List<Tool> interactionsTools = new ArrayList<>();

    for (BaseTool tool : tools.values()) {
      if (tool instanceof FunctionTool functionTool) {
        Optional<FunctionDeclaration> declaration = functionTool.declaration();
        if (declaration.isPresent()) {
          Function.Builder functionBuilder = Function.builder();
          declaration.get().name().ifPresent(functionBuilder::name);
          declaration.get().description().ifPresent(functionBuilder::description);
          declaration.get().parameters().ifPresent(functionBuilder::parameters);
          interactionsTools.add(functionBuilder.build());
        }
      } else if (tool instanceof GoogleSearchTool) {
        interactionsTools.add(GoogleSearch.builder().build());
      } else if (tool instanceof BuiltInCodeExecutionTool) {
        interactionsTools.add(CodeExecution.builder().build());
      }
    }

    return interactionsTools;
  }

  /**
   * Builds CreateInteractionConfig from LlmRequest.
   *
   * @param llmRequest The ADK LlmRequest.
   * @param modelName The model name to use.
   * @param stream Whether to stream the response.
   * @return A CreateInteractionConfig for the Interactions API.
   */
  public static CreateInteractionConfig toCreateInteractionConfig(
      LlmRequest llmRequest, String modelName, boolean stream) {
    CreateInteractionConfig.Builder configBuilder = CreateInteractionConfig.builder();

    // Set model
    configBuilder.model(modelName);

    // Set streaming
    configBuilder.stream(stream);

    // Convert contents to input
    List<Content> contents = llmRequest.contents();
    if (!contents.isEmpty()) {
      if (llmRequest.previousInteractionId().isPresent()) {
        // When chaining, only send the latest user message
        List<Content> latestUserContents = getLatestUserContents(contents);
        if (!latestUserContents.isEmpty()) {
          List<Turn> turns = contentsToTurns(latestUserContents);
          configBuilder.input(Input.fromTurns(turns));
        } else {
          List<Turn> turns = contentsToTurns(contents);
          configBuilder.input(Input.fromTurns(turns));
        }
      } else {
        List<Turn> turns = contentsToTurns(contents);
        configBuilder.input(Input.fromTurns(turns));
      }
    }

    // Set previous interaction ID
    llmRequest.previousInteractionId().ifPresent(configBuilder::previousInteractionId);

    // Set system instruction
    llmRequest
        .config()
        .flatMap(GenerateContentConfig::systemInstruction)
        .flatMap(Content::parts)
        .flatMap(parts -> parts.stream().findFirst())
        .flatMap(Part::text)
        .ifPresent(configBuilder::systemInstruction);

    // Convert tools
    if (!llmRequest.tools().isEmpty()) {
      List<Tool> tools = toInteractionsTools(llmRequest.tools());
      if (!tools.isEmpty()) {
        configBuilder.tools(tools);
      }
    }

    // Set generation config
    llmRequest
        .config()
        .ifPresent(
            cfg -> {
              GenerationConfig.Builder genConfigBuilder = GenerationConfig.builder();
              cfg.temperature().ifPresent(genConfigBuilder::temperature);
              cfg.topP().ifPresent(genConfigBuilder::topP);
              // Note: topK is not supported in Interactions API GenerationConfig
              cfg.maxOutputTokens().ifPresent(genConfigBuilder::maxOutputTokens);
              cfg.stopSequences().ifPresent(genConfigBuilder::stopSequences);
              configBuilder.generationConfig(genConfigBuilder.build());
            });

    return configBuilder.build();
  }

  /**
   * Gets the latest user contents from a list of contents. This is used when chaining interactions
   * to only send the new user message.
   *
   * @param contents The full list of contents.
   * @return The latest user contents (from the last user message to the end).
   */
  public static List<Content> getLatestUserContents(List<Content> contents) {
    // Find the last user content and return from there
    for (int i = contents.size() - 1; i >= 0; i--) {
      Content content = contents.get(i);
      if (content.role().map("user"::equals).orElse(false)) {
        return contents.subList(i, contents.size());
      }
    }
    return contents;
  }

  /**
   * Converts an Interaction response to LlmResponse.
   *
   * <p>This method aligns with the Python ADK's convert_interaction_to_llm_response() function.
   *
   * @param interaction The Interactions API response.
   * @return An LlmResponse.
   */
  public static LlmResponse interactionToLlmResponse(Interaction interaction) {
    LlmResponse.Builder responseBuilder = LlmResponse.builder();

    // Set interaction ID
    responseBuilder.interactionId(interaction.id());

    // Convert outputs to Content
    if (interaction.outputs().isPresent() && !interaction.outputs().get().isEmpty()) {
      Content content = interactionsOutputsToContent(interaction.outputs().get());
      responseBuilder.content(content);
    }

    // Handle error status (check BEFORE checking completion)
    if (interaction.status() == InteractionStatus.FAILED) {
      responseBuilder.errorMessage("Interaction failed");
      responseBuilder.turnComplete(true);
      return responseBuilder.build();
    }

    // Map status to finish reason and turn complete
    // Both COMPLETED and REQUIRES_ACTION mean the model finished its turn
    if (interaction.status() == InteractionStatus.COMPLETED
        || interaction.status() == InteractionStatus.REQUIRES_ACTION) {
      responseBuilder.turnComplete(true);
      responseBuilder.finishReason(new FinishReason(FinishReason.Known.STOP));
    }

    // Map usage to usageMetadata
    interaction
        .usage()
        .ifPresent(
            usage -> {
              GenerateContentResponseUsageMetadata.Builder usageBuilder =
                  GenerateContentResponseUsageMetadata.builder();
              usage.totalInputTokens().ifPresent(usageBuilder::promptTokenCount);
              usage.totalOutputTokens().ifPresent(usageBuilder::candidatesTokenCount);
              // Calculate total if not provided
              int total =
                  usage
                      .totalTokens()
                      .orElseGet(
                          () ->
                              usage.totalInputTokens().orElse(0)
                                  + usage.totalOutputTokens().orElse(0));
              usageBuilder.totalTokenCount(total);
              responseBuilder.usageMetadata(usageBuilder.build());
            });

    // Map model to modelVersion (if present)
    interaction.model().ifPresent(responseBuilder::modelVersion);

    return responseBuilder.build();
  }

  /**
   * Converts Interactions outputs to ADK Content.
   *
   * @param outputs The Interactions outputs.
   * @return An ADK Content object.
   */
  public static Content interactionsOutputsToContent(
      List<com.google.genai.types.interactions.content.Content> outputs) {
    List<Part> parts = new ArrayList<>();

    for (com.google.genai.types.interactions.content.Content output : outputs) {
      if (output instanceof TextContent textContent) {
        textContent.text().ifPresent(text -> parts.add(Part.fromText(text)));
      } else if (output instanceof FunctionCallContent functionCallContent) {
        FunctionCall fc =
            FunctionCall.builder()
                .id(functionCallContent.id())
                .name(functionCallContent.name())
                .args(functionCallContent.arguments())
                .build();
        parts.add(Part.builder().functionCall(fc).build());
      } else if (output instanceof FunctionResultContent functionResultContent) {
        @SuppressWarnings("unchecked")
        Map<String, Object> resultMap =
            functionResultContent.result() instanceof Map
                ? (Map<String, Object>) functionResultContent.result()
                : Map.of("result", functionResultContent.result());
        FunctionResponse fr =
            FunctionResponse.builder()
                .id(functionResultContent.id())
                .name(functionResultContent.name().orElse(""))
                .response(resultMap)
                .build();
        parts.add(Part.builder().functionResponse(fr).build());
      }
    }

    return Content.builder().role("model").parts(parts).build();
  }

  /**
   * Converts a streaming event to LlmResponse.
   *
   * @param event The streaming event.
   * @param interactionId The current interaction ID (may be updated by InteractionEvent).
   * @return An LlmResponse, or empty if the event should be skipped.
   */
  public static Optional<LlmResponse> streamingEventToLlmResponse(
      InteractionStreamingEvent event, String interactionId) {
    LlmResponse.Builder responseBuilder = LlmResponse.builder();
    responseBuilder.interactionId(interactionId);

    if (event instanceof InteractionEvent interactionEvent) {
      // Update interaction ID from event
      if (interactionEvent.interaction().isPresent()) {
        String newId = interactionEvent.interaction().get().id();
        responseBuilder.interactionId(newId);
      }

      if (interactionEvent.isComplete()) {
        responseBuilder.turnComplete(true);
        // Include outputs if present
        if (interactionEvent.interaction().isPresent()
            && interactionEvent.interaction().get().outputs().isPresent()) {
          Content content =
              interactionsOutputsToContent(interactionEvent.interaction().get().outputs().get());
          responseBuilder.content(content);
        }
        return Optional.of(responseBuilder.build());
      } else if (interactionEvent.isStart()) {
        // Just update the interaction ID, don't emit a response yet
        return Optional.empty();
      }
    } else if (event instanceof ContentDelta contentDelta) {
      if (contentDelta.delta().isPresent()) {
        Delta delta = contentDelta.delta().get();
        if (delta instanceof TextDelta textDelta) {
          if (textDelta.text().isPresent()) {
            Content content =
                Content.builder()
                    .role("model")
                    .parts(Part.fromText(textDelta.text().get()))
                    .build();
            responseBuilder.content(content);
            responseBuilder.partial(true);
            return Optional.of(responseBuilder.build());
          }
        } else if (delta instanceof FunctionCallDelta functionCallDelta) {
          // Handle function call delta
          if (functionCallDelta.name().isPresent()) {
            // Arguments in delta are partial JSON strings, use empty map for now
            // Full arguments come in ContentStart event
            FunctionCall fc =
                FunctionCall.builder()
                    .id(functionCallDelta.id().orElse(""))
                    .name(functionCallDelta.name().get())
                    .args(Collections.emptyMap())
                    .build();
            Content content =
                Content.builder()
                    .role("model")
                    .parts(Part.builder().functionCall(fc).build())
                    .build();
            responseBuilder.content(content);
            return Optional.of(responseBuilder.build());
          }
        }
      }
    } else if (event instanceof ContentStart contentStart) {
      // ContentStart events are typically just markers, we may skip them
      if (contentStart.content().isPresent()) {
        com.google.genai.types.interactions.content.Content content = contentStart.content().get();
        if (content instanceof FunctionCallContent functionCallContent) {
          FunctionCall fc =
              FunctionCall.builder()
                  .id(functionCallContent.id())
                  .name(functionCallContent.name())
                  .args(functionCallContent.arguments())
                  .build();
          Content adkContent =
              Content.builder()
                  .role("model")
                  .parts(Part.builder().functionCall(fc).build())
                  .build();
          responseBuilder.content(adkContent);
          return Optional.of(responseBuilder.build());
        }
      }
    }

    return Optional.empty();
  }

  /**
   * Extracts the interaction ID from a streaming event.
   *
   * @param event The streaming event.
   * @return The interaction ID if present in the event.
   */
  public static Optional<String> extractInteractionIdFromEvent(InteractionStreamingEvent event) {
    if (event instanceof InteractionEvent interactionEvent) {
      return interactionEvent.interaction().map(Interaction::id);
    }
    return Optional.empty();
  }

  /**
   * Accumulates streaming text deltas into complete responses.
   *
   * <p>This utility helps combine multiple TextDelta events into a single text response when
   * needed.
   */
  public static class StreamingAccumulator {
    private final StringBuilder textBuffer = new StringBuilder();
    private String interactionId = "";
    private boolean hasContent = false;

    public void addTextDelta(String text) {
      textBuffer.append(text);
      hasContent = true;
    }

    public void setInteractionId(String id) {
      this.interactionId = id;
    }

    public String getInteractionId() {
      return interactionId;
    }

    public boolean hasContent() {
      return hasContent;
    }

    public LlmResponse buildFinalResponse() {
      LlmResponse.Builder builder = LlmResponse.builder();
      builder.interactionId(interactionId);
      builder.turnComplete(true);

      if (textBuffer.length() > 0) {
        Content content =
            Content.builder().role("model").parts(Part.fromText(textBuffer.toString())).build();
        builder.content(content);
      }

      return builder.build();
    }

    public void reset() {
      textBuffer.setLength(0);
      hasContent = false;
    }
  }
}
