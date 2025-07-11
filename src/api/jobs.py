"""
API for managing Databricks jobs.
"""

import logging
from typing import Any, Dict, List, Optional

from src.core.utils import DatabricksAPIError, make_api_request

# Configure logging
logger = logging.getLogger(__name__)


async def create_job(job_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Databricks job.
    
    Args:
        job_config: Job configuration
        
    Returns:
        Response containing the job ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Creating new job")
    return make_api_request("POST", "/api/2.0/jobs/create", data=job_config)


async def run_job(job_id: int, job_parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Run a job now.
    
    Args:
        job_id: ID of the job to run
        job_parameters: Job-level parameters used in the run, e.g., {"param": "overriding_val"}
        
    Returns:
        Response containing the run ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Running job: {job_id}")
    
    run_params = {"job_id": job_id}
    if job_parameters:
        run_params["job_parameters"] = job_parameters
        
    return make_api_request("POST", "/api/2.2/jobs/run-now", data=run_params)


async def list_jobs() -> Dict[str, Any]:
    """
    List all jobs.
    
    Returns:
        Response containing a list of jobs
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info("Listing all jobs")
    return make_api_request("GET", "/api/2.0/jobs/list")


async def get_job(job_id: int) -> Dict[str, Any]:
    """
    Get information about a specific job.
    
    Args:
        job_id: ID of the job
        
    Returns:
        Response containing job information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting information for job: {job_id}")
    return make_api_request("GET", "/api/2.0/jobs/get", params={"job_id": job_id})


async def update_job(job_id: int, new_settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing job.
    
    Args:
        job_id: ID of the job to update
        new_settings: New job settings
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Updating job: {job_id}")
    
    update_data = {
        "job_id": job_id,
        "new_settings": new_settings
    }
    
    return make_api_request("POST", "/api/2.0/jobs/update", data=update_data)


async def delete_job(job_id: int) -> Dict[str, Any]:
    """
    Delete a job.
    
    Args:
        job_id: ID of the job to delete
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Deleting job: {job_id}")
    return make_api_request("POST", "/api/2.0/jobs/delete", data={"job_id": job_id})


async def get_run(run_id: int, include_history: bool = False) -> Dict[str, Any]:
    """
    Get information about a specific job run.
    
    Args:
        run_id: ID of the run
        include_history: Whether to include the run's history in the response
        
    Returns:
        Response containing run information
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting information for run: {run_id}, include_history: {include_history}")
    params = {"run_id": run_id}
    if include_history:
        params["include_history"] = "true"
    return make_api_request("GET", "/api/2.2/jobs/runs/get", params=params)


async def cancel_run(run_id: int) -> Dict[str, Any]:
    """
    Cancel a job run.
    
    Args:
        run_id: ID of the run to cancel
        
    Returns:
        Empty response on success
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Cancelling run: {run_id}")
    return make_api_request("POST", "/api/2.0/jobs/runs/cancel", data={"run_id": run_id})


async def get_run_output(run_id: int) -> Dict[str, Any]:
    """
    Retrieve the output and metadata of a single task run.
    
    When a notebook task returns a value through the dbutils.notebook.exit() call,
    you can use this endpoint to retrieve that value. Databricks restricts this API
    to returning the first 5 MB of the output.
    
    Args:
        run_id: The canonical identifier for the run
        
    Returns:
        Response containing the run output and metadata
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Getting output for run: {run_id}")
    return make_api_request("GET", "/api/2.2/jobs/runs/get-output", params={"run_id": run_id})


async def repair_run(
    run_id: int,
    rerun_tasks: Optional[List[str]] = None,
    rerun_all_failed_tasks: Optional[bool] = None,
    rerun_dependent_tasks: Optional[bool] = None,
    latest_repair_id: Optional[int] = None,
    job_parameters: Optional[Dict[str, Any]] = None,
    pipeline_params: Optional[Dict[str, Any]] = None,
    performance_target: Optional[str] = None
) -> Dict[str, Any]:
    """
    Repair a failed job run by re-running failed tasks and their downstream dependencies.
    
    Args:
        run_id: ID of the run to repair. The run must not be in progress.
        rerun_tasks: Optional list of task keys to rerun.
        rerun_all_failed_tasks: If true, repair all failed tasks. Only one of rerun_tasks or rerun_all_failed_tasks can be used.
        rerun_dependent_tasks: If true, repair all tasks that depend on the tasks in rerun_tasks, even if they were previously successful.
        latest_repair_id: The ID of the latest repair. Not required when repairing a run for the first time, but must be provided on subsequent requests.
        job_parameters: Job-level parameters used in the run, e.g., {"param": "overriding_val"}.
        pipeline_params: Controls whether the pipeline should perform a full refresh.
        performance_target: The performance mode on a serverless job (PERFORMANCE_OPTIMIZED or STANDARD).
        
    Returns:
        Response containing the repair ID
        
    Raises:
        DatabricksAPIError: If the API request fails
    """
    logger.info(f"Repairing run: {run_id}")
    
    repair_data = {"run_id": run_id}
    
    if rerun_tasks is not None:
        repair_data["rerun_tasks"] = rerun_tasks
    
    if rerun_all_failed_tasks is not None:
        repair_data["rerun_all_failed_tasks"] = rerun_all_failed_tasks
    
    if rerun_dependent_tasks is not None:
        repair_data["rerun_dependent_tasks"] = rerun_dependent_tasks
    
    if latest_repair_id is not None:
        repair_data["latest_repair_id"] = latest_repair_id
    
    if job_parameters is not None:
        repair_data["job_parameters"] = job_parameters
    
    if pipeline_params is not None:
        repair_data["pipeline_params"] = pipeline_params
    
    if performance_target is not None:
        repair_data["performance_target"] = performance_target
    
    return make_api_request("POST", "/api/2.2/jobs/runs/repair", data=repair_data) 