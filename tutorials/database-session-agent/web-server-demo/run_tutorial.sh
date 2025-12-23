#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

show_usage() {
  cat << EOF
Usage: $0 [OPTIONS]

Run the Web Server Statistical Agent tutorial with DatabaseSessionService.

OPTIONS:
  -h, --help         Show this help message
  -k, --api-key KEY  Set Gemini API key (default: from GEMINI_API_KEY env var)
  --db TYPE          Database type: h2, postgres, or mysql (default: h2)
  -d, --database URL Set custom database URL (overrides --db)
  --skip-docker      Skip Docker setup (use existing database)
  --port PORT        Web server port (default: 8080)

EXAMPLES:
  # Quick start with H2 (no Docker needed)
  $0
  
  # With PostgreSQL
  $0 --db postgres
  
  # With MySQL
  $0 --db mysql
  
  # Custom database URL
  $0 --database "jdbc:postgresql://myhost:5432/mydb?user=user&password=pass"
  
  # Custom port
  $0 --port 9090

ENVIRONMENT VARIABLES:
  GEMINI_API_KEY     Your Gemini API key (required unless --api-key is set)
  DATABASE_URL       Override default database URL
  SERVER_PORT        Override default server port (8080)

EOF
  exit 0
}

SKIP_DOCKER=false
DB_TYPE="h2"
API_KEY="${GEMINI_API_KEY}"
DATABASE_URL="${DATABASE_URL:-}"
PORT="${SERVER_PORT:-8080}"

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      show_usage
      ;;
    -k|--api-key)
      API_KEY="$2"
      shift 2
      ;;
    -d|--database)
      DATABASE_URL="$2"
      shift 2
      ;;
    --db)
      DB_TYPE="$2"
      shift 2
      ;;
    --skip-docker)
      SKIP_DOCKER=true
      shift
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      show_usage
      ;;
  esac
done

echo "╔════════════════════════════════════════════════════════════╗"
echo "║   Web Server Statistical Agent - DatabaseSessionService   ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

if [ -z "$API_KEY" ]; then
  echo "❌ Error: GEMINI_API_KEY not set"
  echo "   Set it via environment variable or use --api-key flag"
  echo "   Example: export GEMINI_API_KEY=your_key_here"
  exit 1
fi

if [ "$DB_TYPE" = "h2" ]; then
  echo "📦 Using H2 embedded database (file-based)"
  echo "   Location: ./web_server_sessions.mv.db"
  DATABASE_URL=""
  echo ""
elif [ "$SKIP_DOCKER" = false ]; then
  if [ "$DB_TYPE" = "postgres" ]; then
    echo "📦 Setting up PostgreSQL database..."
    CONTAINER_NAME="postgres-adk-web"
    
    if ! docker ps -a --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
      echo "Creating PostgreSQL container..."
      docker run -d \
        --name $CONTAINER_NAME \
        -e POSTGRES_DB=adk_sessions \
        -e POSTGRES_USER=adk_user \
        -e POSTGRES_PASSWORD=adk_password \
        -p 5432:5432 \
        -v postgres-adk-web-data:/var/lib/postgresql/data \
        postgres:latest
      sleep 5
    else
      if ! docker ps --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
        docker start $CONTAINER_NAME
        sleep 3
      fi
    fi
    
    [ -z "$DATABASE_URL" ] && DATABASE_URL="jdbc:postgresql://localhost:5432/adk_sessions?user=adk_user&password=adk_password"
    echo "✓ PostgreSQL running"
    
  elif [ "$DB_TYPE" = "mysql" ]; then
    echo "📦 Setting up MySQL database..."
    CONTAINER_NAME="mysql-adk-web"
    
    if ! docker ps -a --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
      echo "Creating MySQL container..."
      docker run -d \
        --name $CONTAINER_NAME \
        -e MYSQL_ROOT_PASSWORD=root_password \
        -e MYSQL_DATABASE=adk_sessions \
        -e MYSQL_USER=adk_user \
        -e MYSQL_PASSWORD=adk_password \
        -p 3306:3306 \
        -v mysql-adk-web-data:/var/lib/mysql \
        mysql:8.0
      sleep 10
    else
      if ! docker ps --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
        docker start $CONTAINER_NAME
        sleep 5
      fi
    fi
    
    [ -z "$DATABASE_URL" ] && DATABASE_URL="jdbc:mysql://localhost:3306/adk_sessions?user=adk_user&password=adk_password"
    echo "✓ MySQL running"
  fi
fi

echo ""
echo "🔧 Configuration:"
echo "  Database: $DB_TYPE"
if [ -n "$DATABASE_URL" ]; then
  echo "  URL: ${DATABASE_URL/password=*/password=***}"
fi
echo "  Port: $PORT"
echo ""
echo "🚀 Starting web server..."
echo ""

cd "$SCRIPT_DIR"

unset GOOGLE_GENAI_USE_VERTEXAI
unset GOOGLE_CLOUD_PROJECT
unset GOOGLE_APPLICATION_CREDENTIALS
export GEMINI_API_KEY="$API_KEY"
export SERVER_PORT="$PORT"
[ -n "$DATABASE_URL" ] && export DATABASE_URL="$DATABASE_URL"

mvn spring-boot:run
