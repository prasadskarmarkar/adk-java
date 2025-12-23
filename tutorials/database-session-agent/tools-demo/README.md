# Custom FunctionTools Demo with DatabaseSessionService

A tutorial demonstrating **custom FunctionTools** with **@Schema annotations** and **DatabaseSessionService** for session persistence.

## 🎯 What This Tutorial Demonstrates

- ✅ **Custom FunctionTools** - Create tools with `FunctionTool.create()`
- ✅ **@Schema Annotations** - Document tool parameters properly
- ✅ **Tool Calling** - Agent uses tools to accomplish tasks
- ✅ **DatabaseSessionService** - Persist all tool calls to database
- ✅ **Dual Database Support** - H2, PostgreSQL, or MySQL

---

## 🛠️ The 3 Custom Tools

### 1. **validate_dataset**
Validates numerical datasets by checking for:
- Empty datasets
- Insufficient data (<3 values)
- Invalid values (NaN, Infinity)

### 2. **perform_hypothesis_test**
Performs a simple one-sample t-test:
- Tests if mean differs from hypothesized value
- Calculates t-statistic and approximate p-value
- Returns conclusion (reject/fail to reject null hypothesis)

### 3. **export_results**
Exports statistical results to:
- CSV format
- JSON format

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

2. **Run the demo:**
   ```bash
   ./run_tutorial.sh
   ```

3. **With custom dataset:**
   ```bash
   ./run_tutorial.sh "[10, 20, 30, 40, 50]"
   ```

4. **With PostgreSQL:**
   ```bash
   ./run_tutorial.sh --db postgres "[10, 20, 30, 40, 50]"
   ```

---

## 📝 Example Usage

```bash
# Valid dataset (all tools should work)
./run_tutorial.sh "[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]"

# Small dataset (validation warning)
./run_tutorial.sh "[5, 10]"

# With outliers (interesting hypothesis test)
./run_tutorial.sh --db mysql "[10, 12, 11, 13, 12, 11, 200, 10, 12]"
```

---

## 🔍 How It Works

### Custom FunctionTool Definition

```java
public class CustomStatisticalTools {
  
  public static final FunctionTool VALIDATE_DATASET = 
      FunctionTool.create(CustomStatisticalTools.class, "validateDataset");
  
  @Schema(
      name = "validate_dataset",
      description = "Validates a numerical dataset...")
  public static Map<String, Object> validateDataset(
      @Schema(
          name = "dataset",
          description = "Array of numbers as a JSON string")
      String dataset) {
    // Validation logic
    return Map.of("valid", true, "message", "Dataset is valid");
  }
}
```

### Agent with Custom Tools

```java
LlmAgent agent = LlmAgent.builder()
    .name("statistical_tools_demo_agent")
    .model("gemini-2.5-flash")
    .instruction("Use all available tools...")
    .tools(
        CustomStatisticalTools.VALIDATE_DATASET,
        CustomStatisticalTools.PERFORM_HYPOTHESIS_TEST,
        CustomStatisticalTools.EXPORT_RESULTS
    )
    .build();
```

---

## 🎓 What You'll Learn

1. ✅ How to create custom `FunctionTool` with `FunctionTool.create()`
2. ✅ How to use `@Schema` annotations for tool documentation
3. ✅ How to design tool parameters and return values
4. ✅ How DatabaseSessionService persists tool calls and responses
5. ✅ Best practices for tool design

---

## 💡 Tool Design Best Practices

- ✅ **Clear names**: Use descriptive tool and parameter names
- ✅ **Good descriptions**: Help the LLM understand when to use the tool
- ✅ **Validate inputs**: Check for invalid data
- ✅ **Return structured data**: Use Map/JSON for easy parsing
- ✅ **Error handling**: Return error info in the response, don't throw exceptions

---

## 📚 Related Tutorials

- [../seq-multi-agent/](../seq-multi-agent/) - Sequential Pipeline
- [../loop-agent/](../loop-agent/) - Loop Agent with ExitLoopTool
