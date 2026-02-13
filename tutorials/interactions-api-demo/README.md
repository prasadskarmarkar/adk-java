# Interactions API Tutorials

Tutorials demonstrating the Gemini Interactions API with ADK Java.

The Interactions API is an alternative to `generateContent` that chains conversations via
`previousInteractionId`. Instead of resending the full conversation history on every turn,
only the new user message is sent and the server maintains the conversation state.

## Prerequisites

```bash
export GOOGLE_API_KEY=<your-api-key>
# or
export GEMINI_API_KEY=<your-api-key>
```

## Tutorials

| Tutorial | Description | Command |
|----------|-------------|---------|
| **Basic** | Text generation with `useInteractionsApi(true)` | `mvn exec:java@basic -pl tutorials/interactions-api-demo` |
| **Multi-Turn** | Multi-turn conversation with state retention | `mvn exec:java@multi-turn -pl tutorials/interactions-api-demo` |
| **Function Calling** | Tool use (weather, population) via Interactions API | `mvn exec:java@function-calling -pl tutorials/interactions-api-demo` |
| **Streaming** | Streaming (SSE) responses | `mvn exec:java@streaming -pl tutorials/interactions-api-demo` |
| **Code Execution** | Built-in code execution tool | `mvn exec:java@code-execution -pl tutorials/interactions-api-demo` |
| **Google Search** | Google Search grounding via Interactions API | `mvn exec:java@google-search -pl tutorials/interactions-api-demo` |

## Enabling the Interactions API

```java
Gemini gemini = Gemini.builder()
    .modelName("gemini-2.5-flash")
    .useInteractionsApi(true)
    .build();

BaseAgent agent = LlmAgent.builder()
    .name("my_agent")
    .model(gemini)
    .instruction("You are a helpful assistant.")
    .build();
```

## How It Works

1. **First turn**: ADK calls `interactions.create()` with the system instruction and user message.
   The response includes an `interactionId`.

2. **Subsequent turns**: ADK stores the `interactionId` on the event. On the next turn,
   `InteractionsRequestProcessor` finds the most recent `interactionId` and sends it as
   `previousInteractionId`. The server uses this to retrieve the prior conversation state.

3. **Context retention**: The server maintains conversation context server-side, so each request
   only needs the latest user message.
