"""
Salt Master Custom Runner Module: Kafka Runner
==============================================
This RUNNER module orchestrates Kafka DAC (Data Administration & Configuration) tasks
by selecting healthy Kafka broker minions, transferring scripts and configs,
executing them, and cleaning up.

**IMPORTANT**: This is a RUNNER module (runs on Salt Master), not an execution module.

Place this file at: /srv/salt/_runners/kafka_runner.py
                 NOT: /srv/salt/_modules/kafka_runner.py

After placing, sync runners with: sudo salt-run saltutil.sync_runners

Author: DevOps Team
Version: 2.7.0 (Fixed node_rotation command to use --broker-id)
Compatible with: Salt 3000+, Kafka 2.8.2
"""

import logging
import salt.client
import uuid
import time
import os
from typing import Dict, Any, List, Optional, Tuple

# Configure logging
log = logging.getLogger(__name__)

# ============================================================================
# Configuration Constants
# ============================================================================

# Script locations on Salt Master
SCRIPT_BASE_PATH = "/srv/salt/kafka_scripts"

SCRIPT_MAPPINGS = {
    "topic_creation": "topic_creation.py",
    "topic_alteration": "topic_alteration.py",
    "node_rotation": "node_rotation.py",
    "partition_skewness": "partition_skewness.py"
}

# Minion targeting
KAFKA_BROKER_GRAIN = "role"
KAFKA_BROKER_VALUE = "kafka_broker"
KAFKA_BROKER_TARGET = f"G@{KAFKA_BROKER_GRAIN}:{KAFKA_BROKER_VALUE}"

# Timeouts and retries
MINION_PING_TIMEOUT = 10
SCRIPT_EXECUTION_TIMEOUT = 600  # 10 minutes
MAX_BROKER_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# Temp file locations on minions
MINION_TEMP_DIR = "/tmp"


# ============================================================================
# Helper Functions
# ============================================================================

def _get_salt_client() -> salt.client.LocalClient:
    """Create and return a Salt LocalClient instance."""
    try:
        return salt.client.LocalClient()
    except Exception as e:
        log.error(f"Failed to create Salt LocalClient: {e}")
        raise


def _generate_unique_id() -> str:
    """Generate a unique identifier for temporary files."""
    return str(uuid.uuid4())[:13]


def _resolve_script_path(task_type: str) -> Tuple[bool, str, str]:
    """Resolve the script path for a given task type."""
    if task_type not in SCRIPT_MAPPINGS:
        valid_types = ", ".join(SCRIPT_MAPPINGS.keys())
        return False, "", f"Invalid task type '{task_type}'. Valid types: {valid_types}"

    script_name = SCRIPT_MAPPINGS[task_type]
    script_path = os.path.join(SCRIPT_BASE_PATH, script_name)

    try:
        if not os.path.isfile(script_path):
            log.error(f"Script not found at: {script_path}")
            return False, "", f"Script not found at: {script_path}"
        if not os.access(script_path, os.R_OK):
            return False, "", f"Script not readable: {script_path}"
        log.info(f"Script found and readable: {script_path}")
        return True, script_path, ""
    except Exception as e:
        log.error(f"Error checking script path: {e}")
        return False, "", f"Error accessing script: {str(e)}"


def _discover_kafka_brokers(client: salt.client.LocalClient) -> Tuple[bool, List[str], str]:
    """Discover ALL available Kafka broker minions using grain targeting."""
    log.info(f"Discovering all Kafka brokers with target: {KAFKA_BROKER_TARGET}")
    try:
        result = client.cmd(
            KAFKA_BROKER_TARGET,
            'test.ping',
            timeout=MINION_PING_TIMEOUT,
            tgt_type='compound'
        )
        if not result:
            return False, [], "No Kafka broker minions responded to ping"
        responding_minions = list(result.keys())
        log.info(f"Found {len(responding_minions)} responding Kafka broker(s): {responding_minions}")
        return True, responding_minions, ""
    except Exception as e:
        error_msg = f"Failed to discover Kafka brokers: {str(e)}"
        log.error(error_msg)
        return False, [], error_msg


# --- MODIFIED FUNCTION ---
def _find_specific_broker(client: salt.client.LocalClient, broker_target: str) -> Tuple[bool, List[str], str]:
    """
    Find and ping the specific minion.
    Targets by minion ID (hostname) if the target is a FQDN.
    Targets by grain (G@broker_id) if the target is numeric.
    """
    if not broker_target:
        return False, [], "Broker ID or Hostname is required for this task"

    target = ""

    # Check if the target is numeric (broker ID) or a string (hostname)
    if broker_target.isdigit():
        # It's a numeric ID, target by grain
        target = f"G@broker_id:{broker_target}"
        log.info(f"Targeting specific broker minion by grain: {target}")
    else:
        # It's a hostname, target by minion ID
        target = broker_target
        log.info(f"Targeting specific broker minion by ID: {target}")

    try:
        # Ping the specific minion using the dynamic target
        result = client.cmd(
            target,
            'test.ping',
            timeout=MINION_PING_TIMEOUT,
            tgt_type='compound' # 'compound' works for both G@... and minion IDs
        )

        if not result:
            return False, [], f"Minion with target '{target}' did not respond or does not exist."

        responding_minion_list = list(result.keys())
        if len(responding_minion_list) > 1:
            log.warning(f"Found multiple minions with target '{target}': {responding_minion_list}. Using the first one.")

        target_minion = responding_minion_list[0]
        log.info(f"Found responding minion '{target_minion}' for target '{target}'")
        return True, [target_minion], "" # Return as a list for compatibility

    except Exception as e:
        error_msg = f"Failed to find minion with target '{target}': {str(e)}"
        log.error(error_msg)
        return False, [], error_msg
# --- END MODIFIED FUNCTION ---



def _write_file_to_minion(
    client: salt.client.LocalClient, minion_id: str, file_path: str, content: str
) -> Tuple[bool, str]:
    """Write content to a file on a specific minion."""
    log.info(f"Writing file to minion '{minion_id}': {file_path}")
    try:
        result = client.cmd(
            minion_id, 'file.write', [file_path, content], timeout=30
        )
        if minion_id not in result:
            return False, f"No response from minion '{minion_id}'"
        minion_result = result[minion_id]
        if isinstance(minion_result, str) and "Wrote" in minion_result:
            log.info(f"Successfully wrote file: {minion_result}")
            return True, ""
        else:
            return False, f"Unexpected response: {minion_result}"
    except Exception as e:
        error_msg = f"Failed to write file to minion: {str(e)}"
        log.error(error_msg)
        return False, error_msg


def _copy_script_to_minion(
    client: salt.client.LocalClient, minion_id: str, source_script_path: str, dest_script_path: str
) -> Tuple[bool, str]:
    """Copy a script from Salt Master to a minion."""
    log.info(f"Copying script to minion '{minion_id}'")
    log.info(f"  Source: {source_script_path}")
    log.info(f"  Destination: {dest_script_path}")
    try:
        with open(source_script_path, 'r') as f:
            script_content = f.read()
        success, error = _write_file_to_minion(
            client, minion_id, dest_script_path, script_content
        )
        if not success:
            return False, f"Failed to copy script: {error}"
        log.info(f"Making script executable on minion '{minion_id}'")
        chmod_result = client.cmd(
            minion_id, 'file.set_mode', [dest_script_path, '0755'], timeout=10
        )
        if minion_id not in chmod_result:
            log.warning(f"Could not verify chmod on minion '{minion_id}'")
        log.info(f"Script copied successfully to minion '{minion_id}'")
        return True, ""
    except Exception as e:
        error_msg = f"Failed to copy script to minion: {str(e)}"
        log.error(error_msg)
        return False, error_msg


def _cleanup_minion_files(
    client: salt.client.LocalClient, minion_id: str, file_paths: List[str]
) -> None:
    """Clean up temporary files on a minion."""
    log.info(f"Cleaning up temporary files on minion '{minion_id}'")
    for file_path in file_paths:
        try:
            log.debug(f"Removing file: {file_path}")
            client.cmd(minion_id, 'file.remove', [file_path], timeout=10)
        except Exception as e:
            log.warning(f"Error removing file '{file_path}': {e}")
    log.info(f"Cleanup completed on minion '{minion_id}'")


# ============================================================================
# Core Execution Functions (MODIFIED)
# ============================================================================

def _execute_script_on_minion(
    client: salt.client.LocalClient,
    minion_id: str,
    script_path: str,
    yaml_path: str,
    log_level: str,
    dry_run: bool,
    task_type: str,
    broker_id: Optional[str] = None, # The runner arg is still named broker_id
    action: Optional[str] = None
) -> Tuple[bool, Dict[str, Any], str]:
    """
    Execute the Kafka script on a minion with task-specific arguments.
    Note: 'broker_id' param now accepts an ID or a hostname.
    """
    log.info(f"Executing script on minion '{minion_id}'")
    log.info(f"  Task Type: {task_type}")
    log.info(f"  Script: {script_path}")
    log.info(f"  Config: {yaml_path}")
    log.info(f"  Dry Run/Precheck: {dry_run}")

    # --- Build the command dynamically based on task_type ---

    if task_type == "topic_alteration":
        # topic_alteration: python3 script.py --config /path/to/config.yaml
        cmd = f"python3 {script_path} --config {yaml_path}"
        if dry_run:
            cmd += " --dry-run"

    elif task_type == "node_rotation":
        # node_rotation: python3 script.py --config /path/to/config.yaml --broker-id 1001 [--recommission]
        # OR
        # node_rotation: python3 script.py --config /path/to/config.yaml --broker host.example.com [--recommission]
        cmd = f"python3 {script_path} --config {yaml_path}"

        # --- START FIX: Check if broker_id is a number or hostname ---
        if broker_id:
            if broker_id.isdigit():
                cmd += f" --broker-id {broker_id}"
            else:
                cmd += f" --broker {broker_id}"
        # --- END FIX ---

        if action and action != "decommission":
            cmd += f" --{action}"  # Adds --recommission
        if dry_run:
            cmd += " --dry-run"
        log.info(f"  Broker Target: {broker_id}")
        log.info(f"  Action: {action}")

    elif task_type == "partition_skewness":
        # partition_skewness: python3 script.py --config /path/to/config.yaml
        cmd = f"python3 {script_path} --config {yaml_path}"
        if dry_run:
            cmd += " --dry-run"
        else:
            # This is the "execute" command
            cmd += " --execute --monitor --verify"

    else:
        # Default (topic_creation): python3 script.py /path/to/config.yaml INFO
        cmd = f"python3 {script_path} {yaml_path} {log_level}"
        if dry_run:
            # For topic_creation, dry_run=True means --precheck
            cmd += " --precheck"
        log.info(f"  Log Level: {log_level}")

    # --- End command building ---

    log.info(f"Executing command: {cmd}")

    try:
        result = client.cmd(
            minion_id, 'cmd.run_all', [cmd], timeout=SCRIPT_EXECUTION_TIMEOUT
        )
        if minion_id not in result:
            return False, {}, f"No response from minion '{minion_id}'"

        minion_result = result[minion_id]
        retcode = minion_result.get('retcode', 1)
        stdout = minion_result.get('stdout', '')
        stderr = minion_result.get('stderr', '')

        log.info(f"Script execution completed with return code: {retcode}")
        log.debug(f"STDOUT:\n{stdout}")
        if stderr:
            log.warning(f"STDERR:\n{stderr}")

        result_dict = {'retcode': retcode, 'stdout': stdout, 'stderr': stderr}

        if retcode == 0:
            return True, result_dict, ""
        else:
            return False, result_dict, f"Script failed with return code {retcode}"

    except Exception as e:
        error_msg = f"Failed to execute script on minion: {str(e)}"
        log.error(error_msg)
        return False, {}, error_msg


def run_dac_task(
    task_type: str,
    yaml_content: str,
    log_level: str = "INFO",
    dry_run: bool = False,
    broker_id: Optional[str] = None,  # Airflow passes the value as 'broker_id'
    action: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute a Kafka DAC task on a healthy broker minion.

    :param broker_id: For node_rotation, this can be a numeric ID or a hostname/minion_id.
    """
    log.info("=" * 80)
    log.info(f"Starting Kafka DAC Task: {task_type}")
    log.info("=" * 80)
    log.info(f"Log Level: {log_level}")
    log.info(f"Dry Run Mode: {dry_run}")
    log.info(f"Broker Target (from broker_id param): {broker_id}")
    log.info(f"Action: {action}")
    log.info(f"YAML Content Length: {len(yaml_content)} bytes")

    execution_start_time = time.time()
    result = {
        "success": False, "message": "", "minion_id": None,
        "details": {}, "stdout": "", "stderr": ""
    }


    try:
        # Step 1: Validate script
        log.info("Step 1: Validating task type and resolving script")
        success, script_path, error = _resolve_script_path(task_type)
        if not success:
            result["message"] = error
            log.error(f"Script resolution failed: {error}")
            return result

        # Step 2: Create Salt client
        log.info("Step 2: Creating Salt LocalClient")
        try:
            client = _get_salt_client()
        except Exception as e:
            result["message"] = f"Failed to create Salt client: {str(e)}"
            log.error(result["message"])
            return result

        # --- STEP 3: MODIFIED - Find target minions based on task_type ---
        target_minions = []
        if task_type == "node_rotation":
            log.info(f"Step 3: Finding specific broker minion for target {broker_id}")
            # --- EDIT: Call the new function ---
            success, target_minions, error = _find_specific_broker(client, broker_id)
            # --- END EDIT ---
        else:
            log.info("Step 3: Discovering all available Kafka broker minions")
            success, target_minions, error = _discover_kafka_brokers(client)

        if not success:
            result["message"] = error
            log.error(f"Minion discovery failed: {error}")
            return result
        # --- END MODIFICATION ---

        result["details"]["target_minions"] = target_minions
        result["details"]["target_count"] = len(target_minions)

        # Step 4: Generate unique IDs
        unique_id = _generate_unique_id()
        yaml_filename = f"{unique_id}-config.yaml"
        script_filename = f"{unique_id}-{os.path.basename(script_path)}"
        log.info(f"Generated unique ID: {unique_id}")

        # Step 5: Attempt execution
        log.info("Step 5: Attempting execution on target minion(s)")
        execution_successful = False
        last_error = ""

        # --- MODIFIED: Loop over `target_minions` ---
        # For node_rotation, this list will only have one item.
        # For other tasks, it could have many.
        for attempt, minion_id in enumerate(target_minions, start=1):
            log.info("=" * 60)
            log.info(f"Attempt {attempt}/{len(target_minions)}: Using minion '{minion_id}'")
            log.info("=" * 60)


            yaml_path = os.path.join(MINION_TEMP_DIR, yaml_filename)
            script_path_on_minion = os.path.join(MINION_TEMP_DIR, script_filename)
            files_to_cleanup = []

            try:
                # 5a: Write YAML
                log.info(f"Step 5a: Writing YAML config to minion '{minion_id}'")
                success, error = _write_file_to_minion(
                    client, minion_id, yaml_path, yaml_content
                )
                if not success:
                    last_error = f"Failed to write YAML: {error}"
                    log.error(last_error)
                    continue
                files_to_cleanup.append(yaml_path)

                # 5b: Copy script
                log.info(f"Step 5b: Copying script to minion '{minion_id}'")
                success, error = _copy_script_to_minion(
                    client, minion_id, script_path, script_path_on_minion
                )
                if not success:
                    last_error = f"Failed to copy script: {error}"
                    log.error(last_error)
                    _cleanup_minion_files(client, minion_id, files_to_cleanup)
                    continue
                files_to_cleanup.append(script_path_on_minion)

                # 5c: Execute script
                log.info(f"Step 5c: Executing script on minion '{minion_id}'")
                success, exec_result, error = _execute_script_on_minion(
                    client, minion_id, script_path_on_minion,
                    yaml_path, log_level,
                    dry_run=dry_run,
                    task_type=task_type,
                    broker_id=broker_id,  # Pass the target here
                    action=action
                )

                result["stdout"] = exec_result.get("stdout", "")
                result["stderr"] = exec_result.get("stderr", "")
                result["details"]["retcode"] = exec_result.get("retcode", -1)

                # 5d: Cleanup
                log.info(f"Step 5d: Cleaning up temporary files on minion '{minion_id}'")
                _cleanup_minion_files(client, minion_id, files_to_cleanup)

                if success:
                    execution_successful = True
                    result["success"] = True
                    result["minion_id"] = minion_id
                    result["message"] = f"Task '{task_type}' completed successfully on minion '{minion_id}'"
                    log.info("=" * 80)
                    log.info(f"✓ SUCCESS: Task completed on minion '{minion_id}'")
                    log.info("=" * 80)
                    break # Successful execution, exit loop
                else:
                    last_error = f"Script failed on minion '{minion_id}': {error}"
                    log.error(last_error)
                    if attempt < len(target_minions):
                        log.info(f"Retrying with next broker in {RETRY_DELAY_SECONDS} seconds...")
                        time.sleep(RETRY_DELAY_SECONDS)
                        continue

            except Exception as e:
                last_error = f"Unexpected error on minion '{minion_id}': {str(e)}"
                log.error(last_error, exc_info=True)
                _cleanup_minion_files(client, minion_id, files_to_cleanup)
                if attempt < len(target_minions):
                    log.info(f"Retrying with next broker in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue

        if not execution_successful:
            result["message"] = f"Failed on all {len(target_minions)} target minion(s). Last error: {last_error}"
            log.error("=" * 80)
            log.error(f"✗ FAILED: Task failed on all target minion(s)")
            log.error("=" * 80)

    except Exception as e:
        error_msg = f"Unexpected error in run_dac_task: {str(e)}"
        log.error(error_msg, exc_info=True)
        result["message"] = error_msg

    finally:
        execution_time = time.time() - execution_start_time

        result["details"]["execution_time_seconds"] = round(execution_time, 2)
        log.info("=" * 80)
        log.info(f"Task Execution Summary:")
        log.info(f"  Task Type: {task_type}")
        log.info(f"  Success: {result['success']}")
        log.info(f"  Minion: {result['minion_id']}")
        log.info(f"  Execution Time: {execution_time:.2f} seconds")
        log.info("=" * 80)

    return result


# ============================================================================
# Additional Utility Functions (Unchanged)
# ============================================================================

def list_available_tasks() -> List[str]:
    """List all available DAC task types."""
    return list(SCRIPT_MAPPINGS.keys())


def check_broker_health() -> Dict[str, Any]:
    """Check the health of all Kafka broker minions."""
    log.info("Checking Kafka broker health")
    try:
        client = _get_salt_client()
        success, brokers, error = _discover_kafka_brokers(client)
        if not success:
            return {"success": False, "message": error, "brokers": []}
        return {
            "success": True,
            "message": f"Found {len(brokers)} healthy broker(s)",
            "brokers": brokers,
            "broker_count": len(brokers)
        }
    except Exception as e:
        return {"success": False, "message": f"Health check failed: {str(e)}", "brokers": []}


def verify_script_availability(task_type: str) -> Dict[str, Any]:
    """Verify that a script is available for a given task type."""
    log.info(f"Verifying script availability for task: {task_type}")
    success, script_path, error = _resolve_script_path(task_type)
    if success:
        return {"success": True, "message": f"Script found: {script_path}", "script_path": script_path}
    else:
        return {"success": False, "message": error, "script_path": None}
