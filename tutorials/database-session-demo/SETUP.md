# Setup Guide for Database Session Demo

## What We've Done

### 1. Copied database-session-service module
✅ From: `adk-java/contrib/database-session-service`
✅ To: `interactions_api_support/adk-java/contrib/database-session-service`

### 2. Updated parent pom.xml
✅ Added module: `<module>contrib/database-session-service</module>`
✅ Added module: `<module>tutorials/database-session-demo</module>`

### 3. Created tutorial
✅ Created: `tutorials/database-session-demo/`
  - SimpleQuestionAnswerAgent.java
  - DatabaseSessionDemo.java
  - ExportDatabaseToCsv.java
  - pom.xml (references google-adk-database-session-service)
  - README.md

## To Build and Test

### Step 1: Build the entire project (including database-session-service)
```bash
cd ~/Desktop/Prasad/code/adk-java-official/interactions_api_support/adk-java
mvn clean install -DskipTests
```

### Step 2: Setup PostgreSQL
```bash
# Using Docker (recommended)
docker run -d \
  --name postgres-adk \
  -e POSTGRES_USER=adk_user \
  -e POSTGRES_PASSWORD=adk_password \
  -e POSTGRES_DB=adk_test \
  -p 5432:5432 \
  postgres:15

# Verify it's running
docker ps | grep postgres-adk
```

### Step 3: Run the tutorial
```bash
cd tutorials/database-session-demo
mvn exec:java
```

### Step 4 (Optional): Export database to CSV
```bash
mvn exec:java@export-db
ls -l csv_exports/
```

## Troubleshooting

### If build fails on database-session-service
```bash
# Build just that module first
cd contrib/database-session-service
mvn clean install -DskipTests
cd ../..
```

### If PostgreSQL connection fails
```bash
# Check if running
docker ps | grep postgres-adk

# Check logs
docker logs postgres-adk

# Restart if needed
docker restart postgres-adk
```

### If you get "database does not exist"
The tutorial uses defaults from TestDatabaseConfig:
- Database: adk_test
- User: adk_user
- Password: adk_password

Make sure Docker container has these settings!

## Module Dependencies

```
interactions_api_support/adk-java/
├── core/                              (google-adk)
├── dev/                               (google-adk-dev)
├── contrib/
│   └── database-session-service/      (google-adk-database-session-service) ← NEW
└── tutorials/
    └── database-session-demo/         (google-adk-tutorials-database-session-demo) ← NEW
        └── depends on →  database-session-service
```
