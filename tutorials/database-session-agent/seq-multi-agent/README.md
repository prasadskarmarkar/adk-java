# Sequential Multi-Agent Pipeline with DatabaseSessionService

A tutorial demonstrating **SequentialAgent** with **DatabaseSessionService** for persistent agent sessions.

## 🎯 What This Tutorial Demonstrates

- ✅ **SequentialAgent** - Chain 3 specialized agents in a pipeline
- ✅ **State Sharing** - Agents pass data via `outputKey`
- ✅ **DatabaseSessionService** - Persist agent sessions to database
- ✅ **Dual Database Support** - H2 for quick start, PostgreSQL/MySQL for production
- ✅ **Dual API Support** - Gemini API Key or Vertex AI

---

## 📊 The 3-Agent Sequential Pipeline

```
User Input: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    ↓
┌─────────────────────────────┐
│ 1. CentralTendencyAgent     │  → Mean, Median, Mode
│    Model: gemini-2.5-flash  │     outputKey: "central_stats"
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│ 2. DispersionAgent          │  → StdDev, Variance, Range
│    Model: gemini-2.5-flash  │     outputKey: "dispersion_stats"
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│ 3. OutlierDetectorAgent     │  → Z-score outliers + Summary
│    Model: gemini-2.5-flash  │
└─────────────────────────────┘
    ↓
Complete Statistical Report
```

Each agent receives:
- The original dataset
- Output from all previous agents (via state)

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

2. **Run the pipeline:**
   ```bash
   ./run_tutorial.sh
   ```

3. **With custom dataset:**
   ```bash
   ./run_tutorial.sh "[10, 20, 30, 40, 500]"
   ```

4. **With PostgreSQL:**
   ```bash
   ./run_tutorial.sh --db postgres "[10, 20, 30, 40, 50]"
   ```

---

## 📝 Example Usage

```bash
# Default H2 database
./run_tutorial.sh

# MySQL with validation
./run_tutorial.sh --db mysql --validate

# PostgreSQL with outliers
./run_tutorial.sh --db postgres "[10, 12, 11, 13, 12, 11, 200, 10, 12]"
```

---

## 🔍 How It Works

### SequentialAgent Pipeline

```java
SequentialAgent.builder()
    .name("StatisticalAnalysisPipeline")
    .subAgents(
        LlmAgent.builder()
            .name("central_tendency_agent")
            .model("gemini-2.5-flash")
            .outputKey("central_stats")  // ← Saved to state
            .build(),
        
        LlmAgent.builder()
            .name("dispersion_agent")
            .model("gemini-2.5-flash")
            .outputKey("dispersion_stats")  // ← Saved to state
            .build(),
        
        LlmAgent.builder()
            .name("outlier_detector_agent")
            .model("gemini-2.5-flash")
            .build()
    )
    .build();
```

### DatabaseSessionService

```java
DatabaseSessionService sessionService = new DatabaseSessionService(dbUrl);

Runner runner = new Runner(
    sequentialPipeline,
    "statistical_analysis_app",
    new InMemoryArtifactService(),
    sessionService,
    new InMemoryMemoryService()
);

runner.run(userId, sessionId, userContent, runConfig);
```

---

## 🎓 What You'll Learn

1. ✅ How to build sequential agent pipelines
2. ✅ How to use `outputKey` for state sharing between agents
3. ✅ How to persist sessions with DatabaseSessionService
4. ✅ How to configure H2/PostgreSQL/MySQL databases
5. ✅ How sessions survive application restarts

---

## 📚 Related Tutorials

Each agent type now has its own dedicated project:
- [../loop-agent/](../loop-agent/) - Loop Agent with ExitLoopTool for iterative data quality refinement
- [../tools-demo/](../tools-demo/) - Custom statistical FunctionTools (validation, testing, export)
