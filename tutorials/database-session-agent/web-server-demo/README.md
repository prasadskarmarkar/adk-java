# Web Server Statistical Agent with DatabaseSessionService

A web-based tutorial demonstrating **AdkWebServer** with **DatabaseSessionService** for persistent agent sessions via a browser interface.

## 🎯 What This Tutorial Demonstrates

- ✅ **AdkWebServer** - Spring Boot web server with interactive UI
- ✅ **DatabaseSessionService** - Persistent sessions across server restarts
- ✅ **Web UI** - Chat interface for agent interaction
- ✅ **Session Persistence** - Resume conversations after server restart
- ✅ **Multi-Database Support** - H2, PostgreSQL, or MySQL
- ✅ **Statistical Analysis Agent** - Analyze datasets via web interface

---

## 🚀 Quick Start

**Perfect for trying out the web UI with zero setup!**

### Prerequisites
- Java 17+
- Maven 3.6+
- Gemini API Key from [Google AI Studio](https://makersuite.google.com/app/apikey)

### Steps

1. **Set your API key:**
   ```bash
   export GEMINI_API_KEY=your-api-key-here
   ```

2. **Run the server:**
   ```bash
   cd web-server-demo
   ./run_tutorial.sh
   ```

3. **Open your browser:**
   ```
   http://localhost:8080
   ```

4. **Try it out:**
   - Type: "Analyze this dataset: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]"
   - Get statistical analysis (mean, median, std dev, outliers, etc.)
   - Close browser, restart server → your session persists!

**Database:** H2 file at `./web_server_sessions.mv.db`

---

## 🏢 Production Setup (PostgreSQL/MySQL)

### With PostgreSQL

```bash
./run_tutorial.sh --db postgres
```

Starts PostgreSQL in Docker automatically.

### With MySQL

```bash
./run_tutorial.sh --db mysql
```

Starts MySQL in Docker automatically.

### Custom Database

```bash
export DATABASE_URL="jdbc:postgresql://your-host:5432/db?user=user&password=pass"
./run_tutorial.sh --skip-docker
```

---

## 💡 Example Conversations

### Basic Analysis
```
You: Analyze [10, 20, 30, 40, 50]

Agent: I'll analyze this dataset for you!

**Central Tendency:**
- Mean: 30
- Median: 30
...
```

### Outlier Detection
```
You: Find outliers in [10, 12, 11, 13, 12, 11, 200, 10, 12]

Agent: **Outliers Detected:**
- 200 is an outlier (z-score: 5.2)
...
```

---

## 🔑 Key Features

### Persistent Sessions
- Sessions saved to database automatically
- Restart server → conversations resume
- Multi-user support (different browser sessions)

### Statistical Analysis
- Central Tendency (mean, median, mode)
- Dispersion (variance, std dev, range)
- Outlier Detection (z-score method)
- Natural language interface

### Web UI
- Clean chat interface
- Real-time responses
- Session management
- No coding required!

---

## 🛠️ Advanced Options

### Custom Port
```bash
./run_tutorial.sh --port 9090
```

### Database Options
```bash
# H2 (default - no Docker)
./run_tutorial.sh

# PostgreSQL
./run_tutorial.sh --db postgres

# MySQL  
./run_tutorial.sh --db mysql

# Custom URL
./run_tutorial.sh --database "jdbc:..."
```

### Help
```bash
./run_tutorial.sh --help
```

---

## 📊 How It Works

### Architecture

```
Browser (http://localhost:8080)
    ↓
AdkWebServer (Spring Boot)
    ↓
Statistical Agent (Gemini)
    ↓
DatabaseSessionService
    ↓
Database (H2/PostgreSQL/MySQL)
```

### Session Flow

1. User opens browser → creates session
2. User asks question → agent responds
3. Session saved to database
4. Server restarts
5. User returns → session restored!

---

## 🧪 Testing Session Persistence

1. Start server:
   ```bash
   ./run_tutorial.sh
   ```

2. Open browser, chat with agent:
   ```
   You: Analyze [1, 2, 3, 4, 5]
   Agent: [detailed analysis]
   ```

3. Stop server (`Ctrl+C`)

4. Restart server:
   ```bash
   ./run_tutorial.sh
   ```

5. Refresh browser → conversation history restored! ✨

---

## 🐛 Troubleshooting

### Error: "GEMINI_API_KEY not set"
```bash
export GEMINI_API_KEY=your-key-here
```

### Port already in use
```bash
./run_tutorial.sh --port 9090
```

### Database connection issues
```bash
# Check if Docker container is running
docker ps | grep adk-web

# Restart database
docker restart postgres-adk-web  # or mysql-adk-web
```

### Clear database (start fresh)
```bash
# H2
rm web_server_sessions.mv.db

# PostgreSQL/MySQL
docker rm -f postgres-adk-web  # or mysql-adk-web
docker volume rm postgres-adk-web-data
```

---

## 📝 Files

```
web-server-demo/
├── pom.xml                           # Maven config with web server deps
├── run_tutorial.sh                   # Launch script
├── README.md                         # This file
└── src/main/java/com/google/adk/tutorials/
    └── WebServerStatisticalAgent.java   # Main class
```

---

## 🎓 Learn More

This tutorial demonstrates:
- Extending `AdkWebServer` to use `DatabaseSessionService`
- Overriding Spring `@Bean` for custom session service
- Configuring database via environment variables
- Building production-ready agent web applications

**Next Steps:**
- Customize the statistical agent
- Add more tools/capabilities
- Deploy to production with real database
- Add authentication/authorization

---

## 📚 Related Tutorials

- `seq-multi-agent/` - Sequential pipeline (CLI)
- `loop-agent/` - Iterative refinement (CLI)
- `tools-demo/` - Custom tools (CLI)
- `web-server-demo/` - Web UI (this tutorial)

---

## 📄 License

Copyright 2025 Google LLC. Licensed under Apache 2.0.
