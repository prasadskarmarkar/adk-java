# Database Session Service Tutorial

This tutorial demonstrates how to use `DatabaseSessionService` to persist agent conversations to a PostgreSQL database.

## Overview

This example shows:
- ✅ Configuring `DatabaseSessionService` with PostgreSQL
- ✅ Creating a multi-agent sequential pipeline
- ✅ Running conversations with persistent session storage
- ✅ Automatic database schema management via Flyway migrations
- ✅ Exporting session data to CSV for analysis

## Agent Architecture

The tutorial uses a simple **Sequential Agent Pipeline** with two sub-agents:

1. **QuestionAnswerAgent** - Answers factual questions accurately
2. **FactGeneratorAgent** - Provides interesting facts about topics

All conversation history, events, and state are automatically persisted to PostgreSQL tables.

## Prerequisites

### 1. PostgreSQL Database

You need PostgreSQL running locally or accessible via network.

**Option A: Using Docker**
```bash
docker run -d \
  --name postgres-adk \
  -e POSTGRES_USER=adk_user \
  -e POSTGRES_PASSWORD=adk_password \
  -e POSTGRES_DB=adk_test \
  -p 5432:5432 \
  postgres:15
```

**Option B: Local PostgreSQL Installation**
```bash
# macOS
brew install postgresql@15
brew services start postgresql@15

# Ubuntu/Debian
sudo apt-get install postgresql-15
sudo systemctl start postgresql
```

### 2. Create Database and User

```bash
# Connect as postgres superuser
psql -h localhost -U postgres

# Create user and database
CREATE USER adk_user WITH PASSWORD 'adk_password';
CREATE DATABASE adk_test OWNER adk_user;
GRANT ALL PRIVILEGES ON DATABASE adk_test TO adk_user;
\q
```

The schema will be automatically created by Flyway migrations when you first run the tutorial.

### 3. Java and Maven

- Java 17 or higher
- Maven 3.6 or higher

## Configuration

The tutorial uses environment variables with defaults from TestDatabaseConfig:

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL hostname |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `adk_test` | Database name |
| `DB_USER` | `adk_user` | Database username |
| `DB_PASSWORD` | `adk_password` | Database password |

To override defaults:
```bash
export DB_HOST=my-postgres-host
export DB_PORT=5432
export DB_NAME=my_custom_db
export DB_USER=my_user
export DB_PASSWORD=my_password
```

## Running the Tutorial

### Step 1: Run the Demo

```bash
cd tutorials/database-session-demo
mvn clean compile exec:java
```

**What happens:**
1. Connects to PostgreSQL database
2. Initializes `DatabaseSessionService` (auto-runs Flyway migrations)
3. Creates a sequential agent pipeline
4. Sends two prompts:
   - "What is the capital of France?"
   - "Tell me a fun fact about that city"
5. Stores all conversation data in database tables

**Expected Output:**
```
=== DatabaseSessionService Tutorial ===
Database: jdbc:postgresql://localhost:5432/adk_test?user=adk_user&password=adk_password
App Name: database_session_demo
User ID: demo_user

✓ DatabaseSessionService initialized
✓ Runner initialized with SimpleQuestionAnswerAgent

Prompt 1: What is the capital of France?
------------------------------------------------------------
Agent: The capital of France is Paris.

Session ID: <generated-uuid>
Events collected: 4

Prompt 2: Tell me a fun fact about that city
------------------------------------------------------------
Agent: Paris has more than 450 parks and gardens...

Events collected: 4

=== Tutorial Complete ===
Session ID: <generated-uuid>
All conversation data stored in database: adk_test
```

### Step 2: Export Database to CSV

```bash
mvn exec:java@export-db
```

**What happens:**
1. Connects to the database
2. Exports all ADK tables to CSV files in `./csv_exports/`:
   - `adk_sessions.csv` - Session metadata
   - `adk_events.csv` - Complete event history
   - `adk_user_state.csv` - User-specific state
   - `adk_app_state.csv` - Application-level state

**Expected Output:**
```
=== Exporting Database to CSV ===
Database: jdbc:postgresql://localhost:5432/adk_test
Output Directory: ./csv_exports

Found 4 tables: [adk_app_state, adk_events, adk_sessions, adk_user_state]

✓ Exported 0 rows from adk_app_state to ./csv_exports/adk_app_state.csv
✓ Exported 8 rows from adk_events to ./csv_exports/adk_events.csv
✓ Exported 1 rows from adk_sessions to ./csv_exports/adk_sessions.csv
✓ Exported 0 rows from adk_user_state to ./csv_exports/adk_user_state.csv

=== Export Complete ===
```

## Database Schema

DatabaseSessionService creates four main tables:

### `adk_sessions`
Stores session metadata.

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(255) | Unique session ID |
| `app_name` | VARCHAR(255) | Application name |
| `user_id` | VARCHAR(255) | User identifier |
| `created_at` | TIMESTAMP | Session creation time |
| `updated_at` | TIMESTAMP | Last update time |

### `adk_events`
Stores all conversation events.

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(255) | Unique event ID |
| `session_id` | VARCHAR(255) | Parent session ID |
| `author` | VARCHAR(50) | Event author (user/model/agent) |
| `content` | TEXT | Event content (JSON) |
| `created_at` | TIMESTAMP | Event timestamp |
| `sequence` | INTEGER | Event order in session |

### `adk_user_state`
Stores user-specific state (key-value pairs).

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | VARCHAR(255) | Session ID |
| `key` | VARCHAR(255) | State key |
| `value` | TEXT | State value (JSON) |
| `updated_at` | TIMESTAMP | Last update |

### `adk_app_state`
Stores application-level state (key-value pairs).

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | VARCHAR(255) | Session ID |
| `key` | VARCHAR(255) | State key |
| `value` | TEXT | State value (JSON) |
| `updated_at` | TIMESTAMP | Last update |

## Inspecting the Data

### Using psql

```bash
# Connect to database
psql -h localhost -U adk_user -d adk_test

# List all sessions
SELECT id, app_name, user_id, created_at FROM adk_sessions;

# View events for a session
SELECT id, author, created_at, sequence
FROM adk_events
WHERE session_id = '<your-session-id>'
ORDER BY sequence;

# View event content (JSON)
SELECT content FROM adk_events WHERE author = 'model';
```

### Using CSV Files

After running `mvn exec:java@export-db`, you can inspect the CSV files:

```bash
# View all sessions
cat csv_exports/adk_sessions.csv

# View events
cat csv_exports/adk_events.csv

# Count events per session
cut -d',' -f2 csv_exports/adk_events.csv | sort | uniq -c
```

## Customization

### Using Different Database

To use a different PostgreSQL database:

```bash
export DB_NAME=my_custom_database
mvn exec:java
```

### Modifying the Agent

Edit `SimpleQuestionAnswerAgent.java` to:
- Add more sub-agents to the pipeline
- Change agent instructions
- Use different models
- Add tools or custom logic

### Custom Prompts

Edit `DatabaseSessionDemo.java` to change the test prompts:

```java
String[] prompts = {
  "Your custom question 1?",
  "Your custom question 2?"
};
```

## Troubleshooting

### Database Connection Failed

```
Error: FATAL: database "adk_test" does not exist
```

**Solution:** Create the database first:
```bash
psql -h localhost -U postgres -c "CREATE DATABASE adk_test OWNER adk_user;"
```

### PostgreSQL Not Running

```
Error: Connection refused
```

**Solution:** Start PostgreSQL:
```bash
# Docker
docker start postgres-adk

# macOS
brew services start postgresql@15

# Linux
sudo systemctl start postgresql
```

### Permission Denied

```
Error: FATAL: password authentication failed for user "adk_user"
```

**Solution:** Check your credentials and set environment variables:
```bash
export DB_USER=your_username
export DB_PASSWORD=your_password
```

## Next Steps

- Explore the [DatabaseSessionService documentation](../../contrib/database-session-service/README.md)
- Try different database backends (MySQL, Spanner)
- Implement session restoration across service restarts
- Add custom state management logic
- Integrate with your own agent pipelines

## License

Copyright 2025 Google LLC. Licensed under the Apache License, Version 2.0.
