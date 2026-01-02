# Statistical Analysis Pipeline with DatabaseSessionService

A comprehensive tutorial demonstrating **DatabaseSessionService** for persistent agent sessions, featuring **3 different agent patterns** for statistical analysis.

## 🎯 What This Tutorial Demonstrates

- ✅ **DatabaseSessionService** - Persist agent sessions to a database
- ✅ **SequentialAgent** - Chain 3 specialized agents in a pipeline
- ✅ **LoopAgent** - Iterative refinement with `ExitLoopTool`
- ✅ **Custom Tools** - FunctionTool with `@Schema` annotations
- ✅ **State Sharing** - Agents pass data via `outputKey`
- ✅ **Session Restoration** - Resume sessions after restart
- ✅ **Dual Database Support** - H2 for quick start, PostgreSQL for production
- ✅ **Dual API Support** - Gemini API Key or Vertex AI

---

## 🎨 Four Agent Patterns to Choose From

This tutorial includes **four different patterns** demonstrating DatabaseSessionService:

### **Option 1: Sequential Pipeline (Default)**
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
**Demonstrates:** SequentialAgent, outputKey state sharing

### **Option 2: Loop Agent (Iterative Refinement)**
```
User Input: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    ↓
┌─────────────────────────────────────┐
│  LoopAgent (max 5 iterations)       │
│  ┌───────────────────────────────┐  │
│  │ DataQualityRefinementAgent    │  │
│  │ - Assess quality              │  │
│  │ - Identify issues             │  │
│  │ - Suggest improvements        │  │
│  │ - exit_loop when satisfied    │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
    ↓
Iterative Quality Assessment
```
**Demonstrates:** LoopAgent, ExitLoopTool, iterative refinement

### **Option 3: Tool Calls Demo**
```
User Input: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    ↓
┌─────────────────────────────────────┐
│  StatisticalToolsDemoAgent          │
│  Tools:                             │
│  • validate_dataset                 │
│  • perform_hypothesis_test          │
│  • export_results                   │
└─────────────────────────────────────┘
    ↓
Validation → Testing → Export
```
**Demonstrates:** Custom FunctionTools, @Schema annotations, tool usage patterns

### **Option 4: Web Server (Interactive UI)**
```
Browser (http://localhost:8080)
    ↓
┌─────────────────────────────────────┐
│  AdkWebServer + DatabaseSessionService │
│  ┌───────────────────────────────┐  │
│  │ Statistical Analysis Agent    │  │
│  │ - Web UI chat interface       │  │
│  │ - Persistent sessions         │  │
│  │ - Resume after restart        │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
    ↓
Interactive Web Interface
```
**Demonstrates:** AdkWebServer, web UI, session persistence across server restarts

---

## 🚀 Quick Start (Gemini API Key + H2)

**Perfect for learning and local development!**

### Prerequisites
- Java 21+
- Maven 3.6+
- Gemini API Key from [Google AI Studio](https://makersuite.google.com/app/apikey)

### Steps

1. **Set your API key:**
   ```bash
   export GEMINI_API_KEY=your-api-key-here
   ```

2. **Choose and run a tutorial:**

   **Option 1: Sequential Pipeline** (3-agent chain)
   ```bash
   cd seq-multi-agent
   ./run_tutorial.sh
   ```

   **Option 2: Loop Agent** (iterative refinement)
   ```bash
   cd loop-agent
   ./run_tutorial.sh
   ```

   **Option 3: Tool Calls Demo** (custom statistical tools)
   ```bash
   cd tools-demo
   ./run_tutorial.sh
   ```

   **Option 4: Web Server** (interactive web UI)
   ```bash
   cd web-server-demo
   ./run_tutorial.sh
   # Then open http://localhost:8080
   ```

That's it! Each tutorial will:
- ✅ Use H2 database (file: `./stats_analysis_db.mv.db`)
- ✅ Connect to Gemini API via your key
- ✅ Analyze the default dataset
- ✅ Save session to database

### Run Again to See Session Persistence
```bash
./run_tutorial.sh
```
The second run will show that your session was persisted!

---

## 🏢 Production Setup (Vertex AI + PostgreSQL)

**For production deployments with enterprise features.**

### Prerequisites
- Java 21+
- Maven 3.6+
- Docker & Docker Compose
- GCP Project with Vertex AI enabled
- Service Account with Vertex AI permissions

### Steps

1. **Start PostgreSQL:**
   ```bash
   docker-compose up -d
   ```
   This starts PostgreSQL on `localhost:5432`.

2. **Set environment variables:**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   export GOOGLE_GENAI_USE_VERTEXAI=TRUE
   export GOOGLE_CLOUD_PROJECT=your-project-id
   export GOOGLE_CLOUD_LOCATION=us-central1
   export DATABASE_URL="jdbc:postgresql://localhost:5432/adk_sessions?user=adk&password=adk123"
   ```

3. **Run the analysis:**
   ```bash
   mvn compile exec:java
   ```

### Production Features
- ✅ PostgreSQL for robust persistence
- ✅ Vertex AI for enterprise LLM access
- ✅ Multi-instance session sharing
- ✅ SQL query capabilities

---

## 📝 Example Usage

### Default Dataset
```bash
# Sequential Pipeline
cd seq-multi-agent && ./run_tutorial.sh

# Loop Agent
cd loop-agent && ./run_tutorial.sh

# Tool Calls Demo
cd tools-demo && ./run_tutorial.sh
```
Default dataset: `[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`

### Custom Dataset Examples

**Normal distribution (Sequential Pipeline):**
```bash
cd seq-multi-agent
./run_tutorial.sh "[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]"
```

**Outliers detection (Tool Calls Demo):**
```bash
cd tools-demo
./run_tutorial.sh "[10, 12, 11, 13, 12, 11, 200, 10, 12]"
```

**Small dataset quality check (Loop Agent):**
```bash
cd loop-agent
./run_tutorial.sh "[5, 10, 15]"
```

**Negative numbers hypothesis test (Tool Calls Demo):**
```bash
cd tools-demo
./run_tutorial.sh "[-50, -10, 0, 10, 50, 100]"
```

---

## 🗄️ Database Details

### H2 Database (Default)
- **Type**: File-based embedded database
- **Location**: `./stats_analysis_db.mv.db`
- **URL**: `jdbc:h2:./stats_analysis_db`
- **Setup**: None required!
- **Use Case**: Local development, learning

### PostgreSQL (Production)
- **Type**: Standalone database server
- **Port**: 5432
- **Database**: `adk_sessions`
- **User/Password**: `adk` / `adk123` (configurable)
- **Setup**: `docker-compose up -d`
- **Use Case**: Production, multi-instance

### Schema Management
DatabaseSessionService uses **Flyway** for automatic schema migrations:
- ✅ Creates tables on first run
- ✅ Handles schema versioning
- ✅ Safe upgrades across versions

---

## 📂 Project Structure

```
database-session-agent/
├── pom.xml                                # Maven configuration
├── docker-compose.yml                     # PostgreSQL setup
├── .env.example                          # Environment template
├── README.md                             # This file
└── src/main/java/com/google/adk/tutorials/
    ├── StatisticalAnalysisAgent.java     # All 3 agent patterns
    ├── CustomStatisticalTools.java       # Custom FunctionTools
    └── RunStatisticalAnalysis.java       # Main runner with interactive menu
```

---

## 🔍 How It Works

### 1. Sequential Agent Pipeline (StatisticalAnalysisAgent.java)

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

### 2. Loop Agent (StatisticalAnalysisAgent.java)

```java
LlmAgent refinementAgent = LlmAgent.builder()
    .name("data_quality_refinement_agent")
    .model("gemini-2.5-flash")
    .instruction("Iteratively refine data quality assessment...")
    .tools(ExitLoopTool.INSTANCE)  // ← Can exit loop early
    .build();

LoopAgent.builder()
    .name("QualityRefinementLoop")
    .subAgents(refinementAgent)
    .maxIterations(5)  // ← Max 5 iterations
    .build();
```

### 3. Custom Tools (CustomStatisticalTools.java)

```java
public static final FunctionTool VALIDATE_DATASET = 
    FunctionTool.create(CustomStatisticalTools.class, "validateDataset");

@Schema(
    name = "validate_dataset",
    description = "Validates a numerical dataset...")
public static Map<String, Object> validateDataset(
    @Schema(name = "dataset", description = "Array of numbers...") 
    String dataset) {
  // Implementation
}
```

### 4. Interactive Menu (RunStatisticalAnalysis.java)

```java
int agentChoice = getAgentChoice(args);
BaseAgent selectedAgent = selectAgent(agentChoice);
// Routes to STATISTICAL_ANALYSIS_PIPELINE, LOOP_REFINEMENT_AGENT, or TOOL_DEMO_AGENT
```

### 5. DatabaseSessionService Setup (RunStatisticalAnalysis.java)

```java
// Auto-detect database
String dbUrl = System.getenv("DATABASE_URL");
if (dbUrl == null) {
    dbUrl = "jdbc:h2:./stats_analysis_db";  // H2 default
}

DatabaseSessionService sessionService = new DatabaseSessionService(dbUrl);

Runner runner = Runner.builder()
    .sessionService(sessionService)
    .agent(agent)
    .build();

String sessionId = runner.run("[10, 20, 30, 40, 50]");
```

### 3. Session Persistence

Every run saves to the database:
- **Session metadata** (ID, creation time, user)
- **All agent executions** (input, output, model, tokens)
- **Complete state** (central_stats, dispersion_stats, etc.)
- **Conversation history**

---

## 🎓 Learning Objectives

After completing this tutorial, you'll understand:

1. ✅ **How to use DatabaseSessionService** for persistent sessions
2. ✅ **How to build sequential agent pipelines** with state sharing via `outputKey`
3. ✅ **How to create loop agents** with iterative refinement and `ExitLoopTool`
4. ✅ **How to define custom tools** using `FunctionTool` and `@Schema` annotations
5. ✅ **How to configure H2 vs PostgreSQL** for different environments
6. ✅ **How to support both Gemini API and Vertex AI** in the same code
7. ✅ **How sessions survive application restarts**

---

## 🔧 Configuration Reference

### Environment Variables

| Variable | Required | Purpose | Example |
|----------|----------|---------|---------|
| `GEMINI_API_KEY` | For Gemini API | API authentication | `AIza...` |
| `GOOGLE_APPLICATION_CREDENTIALS` | For Vertex AI | Service account path | `/path/to/sa.json` |
| `GOOGLE_GENAI_USE_VERTEXAI` | For Vertex AI | Enable Vertex AI mode | `TRUE` |
| `GOOGLE_CLOUD_PROJECT` | For Vertex AI | GCP project ID | `my-project` |
| `GOOGLE_CLOUD_LOCATION` | For Vertex AI | GCP region | `us-central1` |
| `DATABASE_URL` | Optional | PostgreSQL connection | `jdbc:postgresql://...` |

### Maven Properties

| Property | Default | Purpose |
|----------|---------|---------|
| `exec.mainClass` | `RunStatisticalAnalysis` | Main class to run |
| `exec.args` | `[10, 20, ...]` | Dataset to analyze |

---

## ✅ Validating the Results

After running the tutorial, you can verify that sessions and events were persisted to the database.

### Using PostgreSQL

**1. View recent sessions:**
```bash
psql -U adk_user -d adk_sessions -c "SELECT id, app_name, user_id, create_time FROM sessions ORDER BY create_time DESC LIMIT 5;"
```

**2. Count events per session:**
```bash
psql -U adk_user -d adk_sessions -c "SELECT session_id, COUNT(*) as total_events FROM events GROUP BY session_id ORDER BY MAX(timestamp) DESC LIMIT 5;"
```

**3. View analysis results from latest session:**
```bash
psql -U adk_user -d adk_sessions -c "
SELECT 
  author,
  to_char(timestamp, 'HH24:MI:SS') as time,
  content->'parts'->0->>'text' as message
FROM events 
WHERE session_id = (SELECT id FROM sessions ORDER BY create_time DESC LIMIT 1)
ORDER BY timestamp;"
```

**4. Export results to CSV:**
```bash
psql -U adk_user -d adk_sessions -c "
COPY (
  SELECT 
    author,
    timestamp,
    content->'parts'->0->>'text' as message
  FROM events 
  WHERE session_id = (SELECT id FROM sessions ORDER BY create_time DESC LIMIT 1)
  ORDER BY timestamp
) TO STDOUT WITH CSV HEADER;" > analysis_results.csv
```

### Using H2 (requires H2 Console)

**1. Download H2 Console:**
```bash
curl -o h2.jar https://repo1.maven.org/maven2/com/h2database/h2/2.2.224/h2-2.2.224.jar
```

**2. Start H2 Console:**
```bash
java -jar h2.jar
```

**3. Connect with:**
- **JDBC URL**: `jdbc:h2:./stats_analysis_db`
- **User**: `sa`
- **Password**: *(leave empty)*

**4. Run SQL queries:**
```sql
-- View sessions
SELECT * FROM sessions ORDER BY create_time DESC LIMIT 5;

-- Count events
SELECT session_id, COUNT(*) FROM events GROUP BY session_id;

-- View latest analysis
SELECT author, timestamp, content 
FROM events 
WHERE session_id = (SELECT id FROM sessions ORDER BY create_time DESC LIMIT 1)
ORDER BY timestamp;
```

### Advanced Options

Each tutorial's `run_tutorial.sh` script supports additional flags:

**Validate database after running:**
```bash
cd seq-multi-agent
./run_tutorial.sh --validate
```

**Custom dataset:**
```bash
cd seq-multi-agent
./run_tutorial.sh "[5, 10, 15, 20, 25, 100]"
```

**Combine both:**
```bash
cd tools-demo
./run_tutorial.sh --validate "[10, 12, 11, 13, 12, 11, 200, 10, 12]"
```

**Other options:**
```bash
./run_tutorial.sh --help  # Show all available options
./run_tutorial.sh --db h2  # Use H2 instead of PostgreSQL
./run_tutorial.sh --db mysql  # Use MySQL
./run_tutorial.sh --skip-docker  # Use existing database
```

The `--validate` flag will:
- ✅ Run the tutorial
- ✅ Show session summary
- ✅ Display event counts
- ✅ Print the final analysis output

---

## 🐛 Troubleshooting

### Error: "GEMINI_API_KEY not set"
```bash
export GEMINI_API_KEY=your-api-key-here
```

### Error: "Database connection failed"
```bash
# For PostgreSQL, ensure it's running:
docker-compose ps

# Restart if needed:
docker-compose down
docker-compose up -d
```

### Error: "Could not find artifact google-adk-database-session-service"
```bash
# Build from root of adk-java:
cd ../..
mvn clean install -DskipTests
cd tutorials/database-session-agent
```

### Database file grows too large (H2)
```bash
# Remove old H2 database:
rm stats_analysis_db.mv.db
```

---

## 📚 Related Documentation

- [DatabaseSessionService Documentation](../../contrib/database-session-service/README.md)
- [ADK Core Documentation](../../core/README.md)
- [Sequential Agent Guide](https://google.github.io/adk-java/)
- [Gemini API Keys](https://makersuite.google.com/app/apikey)
- [Vertex AI Setup](https://cloud.google.com/vertex-ai/docs/start/cloud-environment)

---

## 🤝 Contributing

Found an issue or want to improve this tutorial? Please open an issue or PR!

---

## 📄 License

Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0.
