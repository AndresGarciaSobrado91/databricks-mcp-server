from fastmcp import FastMCP
from src.api import clusters, dbfs, jobs, notebooks, sql
from typing import Any, Dict, List, Optional
from mcp.types import TextContent
from src.core.config import settings
import json
import logging

logger = logging.getLogger(__name__)

mcp = FastMCP(
    name="Databricks MCP",
    instructions="This server provides tools to interact with Databricks."
)


# // ---- TOOLS ---- //

# Cluster management tools
@mcp.tool()
async def list_clusters() -> List[TextContent]:
    """List all Databricks clusters"""
    logger.info(f"Listing clusters")
    try:
        result = await clusters.list_clusters()
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error listing clusters: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]

@mcp.tool()
async def create_cluster(cluster_name: str, spark_version: str, node_type_id: str, num_workers: int, autotermination_minutes: int) -> List[TextContent]:
    """Create a new Databricks cluster with parameters: cluster_name (string, required), spark_version (string, required), node_type_id (string, required), num_workers (integer), autotermination_minutes (integer)"""
    logger.info(f"Creating cluster with params: {params}")
    try:
        result = await clusters.create_cluster(params)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error creating cluster: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]

@mcp.tool()
async def terminate_cluster(cluster_id: str) -> List[TextContent]:
    """Terminate a Databricks cluster with parameter: cluster_id (string, required)"""
    logger.info(f"Terminating cluster with params: {cluster_id}")
    try:
        result = await clusters.terminate_cluster(cluster_id)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error terminating cluster: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]

@mcp.tool()
async def get_cluster(cluster_id: str) -> List[TextContent]:
    """Get information about a specific Databricks cluster with parameter: cluster_id (string, required)"""
    logger.info(f"Getting cluster info with params: {cluster_id}")
    try:
        result = await clusters.get_cluster(cluster_id)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error getting cluster info: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]

@mcp.tool()
async def start_cluster(cluster_id: str) -> List[TextContent]:
    """Start a terminated Databricks cluster with parameter: cluster_id (string, required)"""
    logger.info(f"Starting cluster with params: {cluster_id}")
    try:
        result = await clusters.start_cluster(cluster_id)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error starting cluster: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
# Job management tools
@mcp.tool()
async def list_jobs() -> List[TextContent]:
    """List all Databricks jobs"""
    logger.info(f"Listing jobs")
    try:
        result = await jobs.list_jobs()
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error listing jobs: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
@mcp.tool()
async def run_job(job_id: str, job_parameters: Optional[Dict[str, Any]] = None) -> List[TextContent]:
    """Run a Databricks job with parameters: job_id (string, required), job_parameters (dictionary, optional, job-level parameters)"""
    logger.info(f"Running job with params: job_id={job_id}")
    try:
        result = await jobs.run_job(job_id, job_parameters)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error running job: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
@mcp.tool()
async def get_job(job_id: str) -> List[TextContent]:
    """Get information about a specific Databricks job with parameter: job_id (string, required)"""
    logger.info(f"Getting job info with params: {job_id}")
    try:
        result = await jobs.get_job(job_id)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error getting job info: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
@mcp.tool()
async def get_run(run_id: str, include_history: Optional[bool] = False) -> List[TextContent]:
    """Get information about a specific job run with parameters: run_id (string, required), include_history (boolean, optional)"""
    logger.info(f"Getting run info with params: run_id={run_id}, include_history={include_history}")
    try:
        result = await jobs.get_run(run_id, include_history)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error getting run info: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]        

@mcp.tool()
async def get_run_output(run_id: str) -> List[TextContent]:
    """Get the output and metadata of a single task run with parameter: run_id (string, required)"""
    logger.info(f"Getting run output with params: run_id={run_id}")
    try:
        result = await jobs.get_run_output(run_id)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error getting run output: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
@mcp.tool()
async def repair_run(
    run_id: str, 
    rerun_tasks: Optional[List[str]] = None, 
    rerun_all_failed_tasks: Optional[bool] = None,
    rerun_dependent_tasks: Optional[bool] = None,
    latest_repair_id: Optional[int] = None,
    job_parameters: Optional[Dict[str, Any]] = None,
    pipeline_params: Optional[Dict[str, Any]] = None,
    performance_target: Optional[str] = None
) -> List[TextContent]:
    """Repair a failed job run by re-running failed tasks with parameters: 
    run_id (string, required), rerun_tasks (list of strings, optional), rerun_all_failed_tasks (boolean, optional), 
    rerun_dependent_tasks (boolean, optional), latest_repair_id (integer, optional), job_parameters (dictionary, optional), 
    pipeline_params (dictionary, optional), performance_target (string, optional)"""
    logger.info(f"Repairing run with params: run_id={run_id}")
    try:
        result = await jobs.repair_run(
            run_id, 
            rerun_tasks, 
            rerun_all_failed_tasks,
            rerun_dependent_tasks,
            latest_repair_id,
            job_parameters,
            pipeline_params,
            performance_target
        )
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error repairing run: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
# Notebook management tools
@mcp.tool()
async def list_notebooks(path: str) -> List[TextContent]:
    """List notebooks in a workspace directory with parameter: path (string, required)"""
    logger.info(f"Listing notebooks with params: {path}")
    try:
        result = await notebooks.list_notebooks(path)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error listing notebooks: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
@mcp.tool()
async def export_notebook(path: str, format: Optional[str] = "SOURCE") -> List[TextContent]:
    """Export a notebook from the workspace with parameters: path (string, required), format (string, optional, one of: SOURCE, HTML, JUPYTER, DBC)"""
    logger.info(f"Exporting notebook with params: {path}")
    try:
        result = await notebooks.export_notebook(path, format)
                
        # For notebooks, we might want to trim the response for readability
        content = result.get("content", "")
        if len(content) > 1000:
            summary = f"{content[:1000]}... [content truncated, total length: {len(content)} characters]"
            result["content"] = summary
                
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error exporting notebook: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
# DBFS tools
@mcp.tool()
async def list_files(dbfs_path: str) -> List[TextContent]:
    """List files and directories in a DBFS path with parameter: dbfs_path (string, required)"""
    logger.info(f"Listing files with params: {dbfs_path}")
    try:
        result = await dbfs.list_files(dbfs_path)
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]
        
# SQL tools
@mcp.tool()
async def execute_sql(statement: str, warehouse_id: str, catalog: Optional[str] = None, schema: Optional[str] = None) -> List[TextContent]:
    """Execute a SQL statement with parameters: statement (string, required), warehouse_id (string, required), catalog (string, optional), schema (string, optional)"""
    logger.info(f"Executing SQL with params: {statement}")
    try:
        result = await sql.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema
        )
        return [{"text": json.dumps(result)}]
    except Exception as e:
        logger.error(f"Error executing SQL: {str(e)}")
        return [{"text": json.dumps({"error": str(e)})}]


# // ---- END TOOLS ---- //



# // ---- SERVER ---- //

if __name__ == "__main__":
    logger.info("Initializing Databricks MCP server")
    logger.info(f"Databricks host: {settings.DATABRICKS_HOST}")
    logger.info(f"Transport: {settings.TRANSPORT}")

    if settings.TRANSPORT == "stdio":
        mcp.run(transport="stdio")
    else:
        logger.info(f"Server host: {settings.SERVER_HOST}")
        logger.info(f"Server port: {settings.SERVER_PORT}")
        logger.info(f"Path: /sse")
        mcp.run(
            transport=settings.TRANSPORT,
            host=settings.SERVER_HOST,
            port=settings.SERVER_PORT,
            path="/sse",
            log_level="debug"
        )