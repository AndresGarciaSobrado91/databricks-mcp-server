# Databricks MCP Server

This is a server that provides tools to interact with Databricks.

## Features

- Cluster management
- Job management
- Notebook management
- DBFS management
- SQL management

## Installation
Required Python version: 3.11

```bash
# Create and activate virtual environment
uv venv

# On Windows
.\.venv\Scripts\activate

# On Linux/Mac
source .venv/bin/activate

# Install dependencies
uv pip install pip
pip install -r requirements.txt
```

## Configuration

The server can be configured using environment variables or a .env file.

### Environment variables

- `DATABRICKS_HOST`: The Databricks host URL (e.g. https://adb-123456789012345.12.azuredatabricks.net)
- `DATABRICKS_TOKEN`: The Databricks API token
- `TRANSPORT`: The transport to use (e.g. sse). Options: [stdio, sse, streamable-http]. Default: sse
- `SERVER_HOST`: The host to bind the server to (e.g. 0.0.0.0)
- `SERVER_PORT`: The port to bind the server to (e.g. 8000)
- `DEBUG`: Whether to run the server in debug mode (e.g. True)
- `LOG_LEVEL`: The log level (e.g. INFO)

### .env file

Create a .env file in the root directory of the project with the following variables:

```bash
DATABRICKS_HOST=https://adb-123456789012345.12.azuredatabricks.net
DATABRICKS_TOKEN=your_databricks_api_token
TRANSPORT=sse
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
DEBUG=True
LOG_LEVEL=INFO
```

## Obtaining Databricks Credentials

1. Host: Your Databricks instance URL (e.g., your-instance.cloud.databricks.com)
2. Token: Create a personal access token in Databricks:
    - Go to User Settings (click your username in the top right)
    - Select "Developer" tab
    - Click "Manage" under "Access tokens"
    - Generate a new token, and save it (it will not be shown again)

## Running the server in standalone mode

```bash
python -m main
```

## Config Claude-Desktop/Cursor/Windsurf

Add the following to your Claude-Desktop/Cursor config file:   

```json
{
  "mcpServers": {
    "databricks": {
      "command": "uv",
      "args": [
        "--directory",
        "/absolute/path/to/this/project",
        "run",
        "python",
        "-m",
        "main"
      ],
      "env": {
        "DATABRICKS_HOST": "https://your-host.cloud.databricks.com",
        "DATABRICKS_TOKEN": "your-databricks-api-token",
        "TRANSPORT": "stdio"
      }
    }
  }
}
```

If you are using Claude-Desktop, restart it for the changes to take effect.