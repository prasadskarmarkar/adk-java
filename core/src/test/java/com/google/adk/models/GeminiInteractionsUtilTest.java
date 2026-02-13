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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.genai.types.Content;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.Part;
import com.google.genai.types.interactions.GenerationConfig;
import com.google.genai.types.interactions.Input;
import com.google.genai.types.interactions.Interaction;
import com.google.genai.types.interactions.InteractionStatus;
import com.google.genai.types.interactions.content.FunctionCallContent;
import com.google.genai.types.interactions.content.FunctionResultContent;
import com.google.genai.types.interactions.content.TextContent;
import com.google.genai.types.interactions.content.ThoughtContent;
import com.google.genai.types.interactions.streaming.ContentDelta;
import com.google.genai.types.interactions.streaming.ContentStart;
import com.google.genai.types.interactions.streaming.ContentStop;
import com.google.genai.types.interactions.streaming.InteractionEvent;
import com.google.genai.types.interactions.streaming.InteractionSseEvent;
import com.google.genai.types.interactions.streaming.delta.FunctionCallDelta;
import com.google.genai.types.interactions.streaming.delta.TextDelta;
import com.google.genai.types.interactions.streaming.delta.ThoughtSignatureDelta;
import com.google.genai.types.interactions.streaming.delta.ThoughtSummaryDelta;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GeminiInteractionsUtil}. */
@RunWith(JUnit4.class)
public final class GeminiInteractionsUtilTest {

  // =========================================================================
  // convertInteractionsContentToPart — TextContent
  // =========================================================================

  @Test
  public void convertTextContent_createsTextPart() {
    TextContent textContent = TextContent.of("Hello world");

    Optional<Part> result = GeminiInteractionsUtil.convertInteractionsContentToPart(textContent);

    assertThat(result).isPresent();
    assertThat(result.get().text()).hasValue("Hello world");
  }

  @Test
  public void convertTextContent_empty_returnsEmpty() {
    TextContent textContent = TextContent.of("");

    Optional<Part> result = GeminiInteractionsUtil.convertInteractionsContentToPart(textContent);

    assertThat(result).isEmpty();
  }

  // =========================================================================
  // convertInteractionsContentToPart — FunctionCallContent
  // =========================================================================

  @Test
  public void convertFunctionCallContent_createsFunctionCallPart() {
    FunctionCallContent fcContent =
        FunctionCallContent.builder()
            .id("call-1")
            .name("get_weather")
            .arguments(Map.of("city", "London"))
            .build();

    Optional<Part> result = GeminiInteractionsUtil.convertInteractionsContentToPart(fcContent);

    assertThat(result).isPresent();
    assertThat(result.get().functionCall()).isPresent();
    assertThat(result.get().functionCall().get().name()).hasValue("get_weather");
    assertThat(result.get().functionCall().get().id()).hasValue("call-1");
    assertThat(result.get().functionCall().get().args()).isPresent();
  }

  @Test
  public void convertFunctionCallContent_withoutName_stillCreatesPart() {
    FunctionCallContent fcContent =
        FunctionCallContent.builder().id("call-2").arguments(Map.of("x", 1)).build();

    Optional<Part> result = GeminiInteractionsUtil.convertInteractionsContentToPart(fcContent);

    assertThat(result).isPresent();
    assertThat(result.get().functionCall()).isPresent();
    assertThat(result.get().functionCall().get().id()).hasValue("call-2");
  }

  // =========================================================================
  // convertInteractionsContentToPart — FunctionResultContent
  // =========================================================================

  @Test
  public void convertFunctionResultContent_withMapResult_createsFunctionResponsePart() {
    FunctionResultContent frContent =
        FunctionResultContent.builder()
            .id("call-1")
            .name("get_weather")
            .result(Map.of("temperature", 22))
            .build();

    Optional<Part> result = GeminiInteractionsUtil.convertInteractionsContentToPart(frContent);

    assertThat(result).isPresent();
    assertThat(result.get().functionResponse()).isPresent();
    assertThat(result.get().functionResponse().get().name()).hasValue("get_weather");
    assertThat(result.get().functionResponse().get().id()).hasValue("call-1");
  }

  @Test
  public void convertFunctionResultContent_withStringResult_wrapsInMap() {
    FunctionResultContent frContent =
        FunctionResultContent.builder().id("call-2").name("echo").result("hello").build();

    Optional<Part> result = GeminiInteractionsUtil.convertInteractionsContentToPart(frContent);

    assertThat(result).isPresent();
    assertThat(result.get().functionResponse()).isPresent();
    assertThat(result.get().functionResponse().get().response())
        .hasValue(Map.of("result", "hello"));
  }

  // =========================================================================
  // convertInteractionsContentToPart — ThoughtContent
  // =========================================================================

  @Test
  public void convertThoughtContent_withSummary_createsThoughtPart() {
    ThoughtContent thoughtContent = ThoughtContent.of(TextContent.of("some thought"));

    Optional<Part> result = GeminiInteractionsUtil.convertInteractionsContentToPart(thoughtContent);

    assertThat(result).isPresent();
    assertThat(result.get().thought()).hasValue(true);
  }

  @Test
  public void convertThoughtContent_empty_createsEmptyThoughtPart() {
    ThoughtContent thoughtContent = ThoughtContent.builder().build();

    Optional<Part> result = GeminiInteractionsUtil.convertInteractionsContentToPart(thoughtContent);

    assertThat(result).isPresent();
    assertThat(result.get().thought()).hasValue(true);
    assertThat(result.get().text()).hasValue("");
  }

  // =========================================================================
  // convertInteractionToLlmResponse
  // =========================================================================

  @Test
  public void convertInteraction_withTextOutput_setsInteractionIdAndContent() {
    Interaction interaction =
        Interaction.builder()
            .id("interaction-123")
            .status(new InteractionStatus(InteractionStatus.Known.COMPLETED))
            .outputs(TextContent.of("Hello"))
            .build();

    LlmResponse response = GeminiInteractionsUtil.convertInteractionToLlmResponse(interaction);

    assertThat(response.interactionId()).hasValue("interaction-123");
    assertThat(response.content()).isPresent();
    assertThat(response.content().get().parts()).isPresent();
    assertThat(response.content().get().parts().get()).hasSize(1);
    assertThat(response.content().get().parts().get().get(0).text()).hasValue("Hello");
  }

  @Test
  public void convertInteraction_noOutputs_noContent() {
    Interaction interaction =
        Interaction.builder()
            .id("interaction-456")
            .status(new InteractionStatus(InteractionStatus.Known.COMPLETED))
            .build();

    LlmResponse response = GeminiInteractionsUtil.convertInteractionToLlmResponse(interaction);

    assertThat(response.interactionId()).hasValue("interaction-456");
    assertThat(response.content()).isEmpty();
  }

  @Test
  public void convertInteraction_multipleOutputs_allConvertedToParts() {
    Interaction interaction =
        Interaction.builder()
            .id("interaction-multi")
            .status(new InteractionStatus(InteractionStatus.Known.COMPLETED))
            .outputs(
                TextContent.of("Hello"),
                FunctionCallContent.builder().id("fc-1").name("do_something").build())
            .build();

    LlmResponse response = GeminiInteractionsUtil.convertInteractionToLlmResponse(interaction);

    assertThat(response.content()).isPresent();
    assertThat(response.content().get().parts().get()).hasSize(2);
    assertThat(response.content().get().parts().get().get(0).text()).hasValue("Hello");
    assertThat(response.content().get().parts().get().get(1).functionCall()).isPresent();
  }

  @Test
  public void convertInteraction_contentRoleIsModel() {
    Interaction interaction =
        Interaction.builder()
            .id("interaction-role")
            .status(new InteractionStatus(InteractionStatus.Known.COMPLETED))
            .outputs(TextContent.of("Hi"))
            .build();

    LlmResponse response = GeminiInteractionsUtil.convertInteractionToLlmResponse(interaction);

    assertThat(response.content().get().role()).hasValue("model");
  }

  // =========================================================================
  // convertInteractionEventToLlmResponse — streaming SSE events
  // =========================================================================

  @Test
  public void convertContentStop_returnsEmpty() {
    ContentStop event = ContentStop.builder().build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isEmpty();
  }

  @Test
  public void convertContentDelta_textDelta_returnsPartialTextResponse() {
    TextDelta textDelta = TextDelta.builder().text("chunk").build();
    ContentDelta event = ContentDelta.builder().delta(textDelta).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isPresent();
    assertThat(result.get().partial()).hasValue(true);
    assertThat(result.get().content()).isPresent();
    assertThat(result.get().content().get().parts().get().get(0).text()).hasValue("chunk");
  }

  @Test
  public void convertContentDelta_emptyTextDelta_returnsEmpty() {
    TextDelta textDelta = TextDelta.builder().text("").build();
    ContentDelta event = ContentDelta.builder().delta(textDelta).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isEmpty();
  }

  @Test
  public void convertContentDelta_functionCallDelta_returnsPartialFunctionCallResponse() {
    FunctionCallDelta fcDelta =
        FunctionCallDelta.builder()
            .name("get_weather")
            .id("fc-1")
            .arguments(Map.of("city", "Paris"))
            .build();
    ContentDelta event = ContentDelta.builder().delta(fcDelta).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isPresent();
    assertThat(result.get().partial()).hasValue(true);
    assertThat(result.get().content().get().parts().get().get(0).functionCall()).isPresent();
    assertThat(result.get().content().get().parts().get().get(0).functionCall().get().name())
        .hasValue("get_weather");
  }

  @Test
  public void convertContentDelta_functionCallDelta_noNameNoArgs_returnsEmpty() {
    FunctionCallDelta fcDelta = FunctionCallDelta.builder().build();
    ContentDelta event = ContentDelta.builder().delta(fcDelta).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isEmpty();
  }

  @Test
  public void convertContentDelta_thoughtSummaryDelta_returnsThoughtPart() {
    ThoughtSummaryDelta thoughtDelta = ThoughtSummaryDelta.builder().build();
    ContentDelta event = ContentDelta.builder().delta(thoughtDelta).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isPresent();
    assertThat(result.get().partial()).hasValue(true);
    assertThat(result.get().content().get().parts().get().get(0).thought()).hasValue(true);
  }

  @Test
  public void convertContentDelta_thoughtSignatureDelta_returnsEmpty() {
    ThoughtSignatureDelta sigDelta = ThoughtSignatureDelta.builder().build();
    ContentDelta event = ContentDelta.builder().delta(sigDelta).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isEmpty();
  }

  @Test
  public void convertContentDelta_noDelta_returnsEmpty() {
    ContentDelta event = ContentDelta.builder().build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isEmpty();
  }

  @Test
  public void convertContentStart_functionCall_returnsPartialFunctionCallResponse() {
    FunctionCallContent fcContent =
        FunctionCallContent.builder()
            .id("fc-start-1")
            .name("search")
            .arguments(Map.of("q", "test"))
            .build();
    ContentStart event = ContentStart.builder().content(fcContent).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isPresent();
    assertThat(result.get().partial()).hasValue(true);
    assertThat(result.get().content().get().parts().get().get(0).functionCall()).isPresent();
  }

  @Test
  public void convertContentStart_thoughtContent_returnsEmpty() {
    ThoughtContent thought = ThoughtContent.builder().build();
    ContentStart event = ContentStart.builder().content(thought).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isEmpty();
  }

  @Test
  public void convertContentStart_noContent_returnsEmpty() {
    ContentStart event = ContentStart.builder().build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse(event);

    assertThat(result).isEmpty();
  }

  @Test
  public void convertInteractionEvent_complete_withInteraction_setsTurnComplete() {
    Interaction interaction =
        Interaction.builder()
            .id("ie-complete")
            .status(new InteractionStatus(InteractionStatus.Known.COMPLETED))
            .outputs(TextContent.of("Done"))
            .build();
    InteractionEvent event =
        InteractionEvent.builder()
            .eventType("interaction.complete")
            .interaction(interaction)
            .build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse((InteractionSseEvent) event);

    assertThat(result).isPresent();
    assertThat(result.get().turnComplete()).hasValue(true);
    assertThat(result.get().interactionId()).hasValue("ie-complete");
  }

  @Test
  public void convertInteractionEvent_complete_requiresAction_noTurnComplete() {
    Interaction interaction =
        Interaction.builder()
            .id("ie-action")
            .status(new InteractionStatus(InteractionStatus.Known.REQUIRES_ACTION))
            .outputs(FunctionCallContent.builder().id("fc-1").name("do_something").build())
            .build();
    InteractionEvent event =
        InteractionEvent.builder()
            .eventType("interaction.complete")
            .interaction(interaction)
            .build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse((InteractionSseEvent) event);

    assertThat(result).isPresent();
    assertThat(result.get().turnComplete()).isEmpty();
  }

  @Test
  public void convertInteractionEvent_start_extractsInteractionId() {
    Interaction interaction =
        Interaction.builder()
            .id("ie-start-123")
            .status(new InteractionStatus(InteractionStatus.Known.IN_PROGRESS))
            .build();
    InteractionEvent event =
        InteractionEvent.builder().eventType("interaction.start").interaction(interaction).build();

    Optional<LlmResponse> result =
        GeminiInteractionsUtil.convertInteractionEventToLlmResponse((InteractionSseEvent) event);

    assertThat(result).isPresent();
    assertThat(result.get().interactionId()).hasValue("ie-start-123");
  }

  // =========================================================================
  // convertContentsToInput
  // =========================================================================

  @Test
  public void convertContentsToInput_empty_returnsInput() {
    Input result = GeminiInteractionsUtil.convertContentsToInput(ImmutableList.of());

    assertThat(result).isNotNull();
  }

  @Test
  public void convertContentsToInput_singleUserText_returnsInput() {
    List<Content> contents =
        ImmutableList.of(Content.builder().role("user").parts(Part.fromText("Hello")).build());

    Input result = GeminiInteractionsUtil.convertContentsToInput(contents);

    assertThat(result).isNotNull();
  }

  @Test
  public void convertContentsToInput_multiTurn_returnsInput() {
    List<Content> contents =
        ImmutableList.of(
            Content.builder().role("user").parts(Part.fromText("Hi")).build(),
            Content.builder().role("model").parts(Part.fromText("Hello")).build(),
            Content.builder().role("user").parts(Part.fromText("How are you?")).build());

    Input result = GeminiInteractionsUtil.convertContentsToInput(contents);

    assertThat(result).isNotNull();
  }

  @Test
  public void convertContentsToInput_singleUserMultiPart_returnsInput() {
    List<Content> contents =
        ImmutableList.of(
            Content.builder()
                .role("user")
                .parts(Part.fromText("part1"), Part.fromText("part2"))
                .build());

    Input result = GeminiInteractionsUtil.convertContentsToInput(contents);

    assertThat(result).isNotNull();
  }

  @Test
  public void convertContentsToInput_withFunctionCall_returnsInput() {
    List<Content> contents =
        ImmutableList.of(
            Content.builder()
                .role("model")
                .parts(
                    Part.builder()
                        .functionCall(
                            com.google.genai.types.FunctionCall.builder()
                                .name("fn")
                                .id("c1")
                                .build())
                        .build())
                .build(),
            Content.builder()
                .role("user")
                .parts(
                    Part.builder()
                        .functionResponse(
                            com.google.genai.types.FunctionResponse.builder()
                                .name("fn")
                                .id("c1")
                                .response(Map.of("result", "ok"))
                                .build())
                        .build())
                .build());

    Input result = GeminiInteractionsUtil.convertContentsToInput(contents);

    assertThat(result).isNotNull();
  }

  // =========================================================================
  // extractSystemInstruction
  // =========================================================================

  @Test
  public void extractSystemInstruction_present_returnsText() {
    GenerateContentConfig config =
        GenerateContentConfig.builder()
            .systemInstruction(
                Content.builder().parts(Part.fromText("You are a helpful assistant.")).build())
            .build();

    Optional<String> result = GeminiInteractionsUtil.extractSystemInstruction(Optional.of(config));

    assertThat(result).hasValue("You are a helpful assistant.");
  }

  @Test
  public void extractSystemInstruction_multipleParts_joinedWithNewline() {
    GenerateContentConfig config =
        GenerateContentConfig.builder()
            .systemInstruction(
                Content.builder()
                    .parts(Part.fromText("Line one"), Part.fromText("Line two"))
                    .build())
            .build();

    Optional<String> result = GeminiInteractionsUtil.extractSystemInstruction(Optional.of(config));

    assertThat(result).hasValue("Line one\nLine two");
  }

  @Test
  public void extractSystemInstruction_emptyConfig_returnsEmpty() {
    Optional<String> result = GeminiInteractionsUtil.extractSystemInstruction(Optional.empty());

    assertThat(result).isEmpty();
  }

  @Test
  public void extractSystemInstruction_noSystemInstruction_returnsEmpty() {
    GenerateContentConfig config = GenerateContentConfig.builder().build();

    Optional<String> result = GeminiInteractionsUtil.extractSystemInstruction(Optional.of(config));

    assertThat(result).isEmpty();
  }

  // =========================================================================
  // buildGenerationConfig
  // =========================================================================

  @Test
  public void buildGenerationConfig_emptyConfig_returnsEmpty() {
    Optional<GenerationConfig> result =
        GeminiInteractionsUtil.buildGenerationConfig(Optional.empty());

    assertThat(result).isEmpty();
  }

  @Test
  public void buildGenerationConfig_noFieldsSet_returnsEmpty() {
    GenerateContentConfig config = GenerateContentConfig.builder().build();

    Optional<GenerationConfig> result =
        GeminiInteractionsUtil.buildGenerationConfig(Optional.of(config));

    assertThat(result).isEmpty();
  }

  @Test
  public void buildGenerationConfig_temperatureSet_returnsConfig() {
    GenerateContentConfig config = GenerateContentConfig.builder().temperature(0.7f).build();

    Optional<GenerationConfig> result =
        GeminiInteractionsUtil.buildGenerationConfig(Optional.of(config));

    assertThat(result).isPresent();
  }

  @Test
  public void buildGenerationConfig_multipleFieldsSet_returnsConfig() {
    GenerateContentConfig config =
        GenerateContentConfig.builder().temperature(0.5f).topP(0.9f).maxOutputTokens(1024).build();

    Optional<GenerationConfig> result =
        GeminiInteractionsUtil.buildGenerationConfig(Optional.of(config));

    assertThat(result).isPresent();
  }

  @Test
  public void buildGenerationConfig_stopSequencesSet_returnsConfig() {
    GenerateContentConfig config =
        GenerateContentConfig.builder().stopSequences(ImmutableList.of("STOP", "END")).build();

    Optional<GenerationConfig> result =
        GeminiInteractionsUtil.buildGenerationConfig(Optional.of(config));

    assertThat(result).isPresent();
  }

  // =========================================================================
  // getLatestUserContents
  // =========================================================================

  @Test
  public void getLatestUserContents_returnsFromLastUserContent() {
    List<Content> contents =
        ImmutableList.of(
            Content.builder().role("user").parts(Part.fromText("Hi")).build(),
            Content.builder().role("model").parts(Part.fromText("Hello")).build(),
            Content.builder().role("user").parts(Part.fromText("How are you?")).build());

    List<Content> result = GeminiInteractionsUtil.getLatestUserContents(contents);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).role()).hasValue("user");
    assertThat(result.get(0).parts().get().get(0).text()).hasValue("How are you?");
  }

  @Test
  public void getLatestUserContents_withFunctionResponse_includesTrailingContents() {
    Content modelContent =
        Content.builder()
            .role("model")
            .parts(
                Part.builder()
                    .functionCall(
                        com.google.genai.types.FunctionCall.builder()
                            .name("get_weather")
                            .id("call-1")
                            .build())
                    .build())
            .build();
    Content userContent =
        Content.builder()
            .role("user")
            .parts(
                Part.builder()
                    .functionResponse(
                        com.google.genai.types.FunctionResponse.builder()
                            .name("get_weather")
                            .id("call-1")
                            .build())
                    .build())
            .build();

    List<Content> contents =
        ImmutableList.of(
            Content.builder().role("user").parts(Part.fromText("Hi")).build(),
            Content.builder().role("model").parts(Part.fromText("Hello")).build(),
            modelContent,
            userContent);

    List<Content> result = GeminiInteractionsUtil.getLatestUserContents(contents);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).role()).hasValue("user");
  }

  @Test
  public void getLatestUserContents_empty_returnsEmpty() {
    List<Content> result = GeminiInteractionsUtil.getLatestUserContents(ImmutableList.of());

    assertThat(result).isEmpty();
  }

  @Test
  public void getLatestUserContents_noUserContent_returnsAll() {
    List<Content> contents =
        ImmutableList.of(Content.builder().role("model").parts(Part.fromText("Hello")).build());

    List<Content> result = GeminiInteractionsUtil.getLatestUserContents(contents);

    assertThat(result).hasSize(1);
  }

  @Test
  public void getLatestUserContents_userAtStart_returnsFromStart() {
    List<Content> contents =
        ImmutableList.of(
            Content.builder().role("user").parts(Part.fromText("Hi")).build(),
            Content.builder().role("model").parts(Part.fromText("Hello")).build());

    List<Content> result = GeminiInteractionsUtil.getLatestUserContents(contents);

    // Walks backwards, finds "user" at index 0, returns subList(0, 2) = everything
    assertThat(result).hasSize(2);
  }
}
