# Loop Agent with ExitLoopTool and DatabaseSessionService

A tutorial demonstrating **LoopAgent** with **ExitLoopTool** for iterative refinement and **DatabaseSessionService** for session persistence.

## 🎯 What This Tutorial Demonstrates

- ✅ **LoopAgent** - Iterative agent execution with max iterations
- ✅ **ExitLoopTool** - Early loop termination when conditions are met
- ✅ **Iterative Refinement** - Data quality assessment improves over iterations
- ✅ **DatabaseSessionService** - Persist all loop iterations to database
- ✅ **Dual Database Support** - H2, PostgreSQL, or MySQL

---

## 🔄 The Loop Agent Pattern

```
User Input: [10, 20, 30, 40, 50]
    ↓
┌─────────────────────────────────────────────┐
│  LoopAgent (max 5 iterations)               │
│  ┌───────────────────────────────────────┐  │
│  │ DataQualityRefinementAgent            │  │
│  │ - Iteration 1: Initial assessment     │  │
│  │ - Iteration 2: Refined analysis       │  │
│  │ - Iteration 3: Detailed check         │  │
│  │ - exit_loop when quality >= 80        │  │
│  └───────────────────────────────────────┘  │
└─────────────────────────────────────────────┘
    ↓
Iterative Quality Report
```

The agent:
- Runs up to 5 iterations
- Can exit early via `ExitLoopTool`
- Each iteration refines the previous analysis
- All iterations persisted to database

---

## 🚀 Quick Start

### Prerequisites
- Java 21+
- Maven 3.6+
- Gemini API Key from [Google AI Studio](https://makersuite.google.com/app/apikey)

### Steps

1. **Set your API key:**
   ```bash
   export GEMINI_API_KEY=your-api-key-here
   ```

2. **Run the loop:**
   ```bash
   ./run_tutorial.sh
   ```

3. **With custom dataset:**
   ```bash
   ./run_tutorial.sh "[5, 10, 15]"
   ```

4. **With PostgreSQL:**
   ```bash
   ./run_tutorial.sh --db postgres "[10, 20, 30, 40, 50]"
   ```

---

## 📝 Example Usage

```bash
# Small dataset (may exit early)
./run_tutorial.sh "[5, 10, 15]"

# Complex dataset (may use all 5 iterations)
./run_tutorial.sh "[10, 12, 11, 13, 12, 11, 200, 10, 12, 999]"

# With MySQL and validation
./run_tutorial.sh --db mysql --validate
```

---

## 🔍 How It Works

### LoopAgent with ExitLoopTool

```java
LlmAgent refinementAgent = LlmAgent.builder()
    .name("data_quality_refinement_agent")
    .model("gemini-2.5-flash")
    .instruction("Iteratively assess data quality...")
    .tools(ExitLoopTool.INSTANCE)  // ← Can exit loop early
    .build();

LoopAgent loopAgent = LoopAgent.builder()
    .name("QualityRefinementLoop")
    .subAgents(refinementAgent)
    .maxIterations(5)  // ← Maximum 5 iterations
    .build();
```

### DatabaseSessionService

```java
DatabaseSessionService sessionService = new DatabaseSessionService(dbUrl);

Runner runner = new Runner(
    loopAgent,
    "loop_quality_refinement_app",
    new InMemoryArtifactService(),
    sessionService,
    new InMemoryMemoryService()
);

// All loop iterations are persisted!
runner.run(userId, sessionId, userContent, runConfig);
```

---

## 🎓 What You'll Learn

1. ✅ How to create LoopAgent with `maxIterations`
2. ✅ How to use `ExitLoopTool` for early termination
3. ✅ How to implement iterative refinement patterns
4. ✅ How DatabaseSessionService persists all loop iterations
5. ✅ When to use LoopAgent vs SequentialAgent

---

## 💡 Loop Termination

The loop terminates when:
- ✅ Agent calls `exit_loop` tool (early exit)
- ✅ `maxIterations` is reached (5 iterations)
- ✅ Agent escalates (error condition)

---

## 📚 Related Tutorials

- [../seq-multi-agent/](../seq-multi-agent/) - Sequential Pipeline
- [../tools-demo/](../tools-demo/) - Custom FunctionTools
