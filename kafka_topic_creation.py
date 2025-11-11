"""
Airflow DAG for Kafka Topic Creation via Salt-Stack (Modular Version)
======================================================================
Restructured with clear separation of concerns for better manageability.
Now uses Airflow Connections for Salt credentials.

Tasks:
1. validate_input - Validates user configuration
2. generate_yaml - Converts config to YAML string
3. run_precheck - Runs the topic creation script with --precheck
4. execute_creation - Runs the final topic creation
5. verify_result - Verifies the execution result
6. generate_report - Creates final report

Author: DevOps Team
Version: 3.1.0 (Added Slack alerting on failure)
Compatible with: Airflow 3.0+, Salt 3000+, Kafka 2.8.2
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Param
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook  # <-- IMPORTED
from datetime import datetime, timedelta
import requests
import json
import yaml
import logging
import re
import copy  # <-- Added for deepcopy
from typing import Dict, Any, Optional, Tuple

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Constants
# ============================================================================

# --- NEW: Airflow Connection IDs ---
SALT_CONN_ID = "salt_api_default"
SLACK_CONN_ID = "slack_webhook_default"  # <-- ADDED FOR SLACK ALERTS
# --- END NEW ---

DAC_TASK_TYPE = "topic_creation"
REQUEST_TIMEOUT = 300

# --- EDIT: Restored full DEFAULT_CONFIG ---
# These are the defaults that will be used if a user provides no optional values
DEFAULT_CONFIG = {
    "topic_name": "example.events.prod", # Will be overwritten by mandatory param
    "partitions": 1,                     # Will be overwritten by mandatory param
    "replication_factor": 1,             # Will be overwritten by mandatory param
    "bootstrap_servers": ["stg-hdpashique101:6667"], # Will be overwritten by mandatory param
    "log_level": "INFO",
    "monitoring": { # Will be overwritten by mandatory param
        "opentsdb_url": "http://opentsdb-read-no-dp-limit.nixy.stg-drove.phonepe.nb6/api/query"
    },
    "config": {
        "cleanup.policy": "delete",
        "retention.ms": 604800000,
        "retention.bytes": 10737418240,
        "min.insync.replicas": 2,
        "unclean.leader.election.enable": False,
        "delete.retention.ms": 86400000,
        "segment.ms": 604800000,
        "segment.bytes": 1073741824,
        "compression.type": "producer",
        "max.message.bytes": 1048576
    }
}
# --- END EDIT ---

# ============================================================================
# Helper Functions
# ============================================================================

def _log_multiline(output_string: str, prefix: str = "  > "):
    """
    Helper to print a multiline string for readable Airflow logs.
    Strips the script's own log prefix for clarity.
    """
    if not output_string:
        print(f"{prefix}(No script output received)")
        return
    
    lines = output_string.strip().split('\n')
    for line in lines:
        line_to_print = line.strip()
        if not line_to_print:  # Skip empty lines
            continue
            
        parts = line_to_print.split(' - ', 2)
        
        if len(parts) == 3:
            message = parts[2].strip()
            if message:
                print(f"{prefix}{message}")
        else:
            print(f"{prefix}{line_to_print}")


def validate_topic_name(topic_name: str) -> Tuple[bool, str]:
    """Validate Kafka topic name according to Kafka conventions."""
    if not topic_name or len(topic_name) == 0:
        return False, "Topic name cannot be empty"

    if len(topic_name) > 249:
        return False, "Topic name exceeds 249 characters"

    valid_pattern = re.compile(r'^[a-zA-Z0-9._-]+$')
    if not valid_pattern.match(topic_name):
        return False, "Topic name must contain only ASCII alphanumerics, '.', '_', '-'"

    if topic_name in ['.', '..']:
        return False, "Topic name cannot be '.' or '..'"

    return True, ""


def validate_numeric_value(value: Any, name: str, min_val: int = 1, max_val: Optional[int] = None) -> Tuple[bool, str]:
    """Validate numeric configuration values."""
    if not isinstance(value, int):
        return False, f"{name} must be an integer"

    if value < min_val:
        return False, f"{name} must be at least {min_val}"

    if max_val and value > max_val:
        return False, f"{name} exceeds maximum of {max_val}"

    return True, ""


# --- NEW: SLACK ALERTING FUNCTION ---
def _send_slack_alert_on_failure(context: Dict[str, Any]):
    """
    Custom failure callback function that sends a formatted Slack alert.
    Fetches the webhook URL from the 'slack_webhook_default' connection.
    """
    logger.info(f"Task failed, initiating Slack alert notification...")
    
    try:
        # --- 1. Get Slack Connection ---
        logger.info(f"Fetching Slack webhook from connection: {SLACK_CONN_ID}")
        slack_conn = BaseHook.get_connection(SLACK_CONN_ID)
        
        # Assumes 'Extra' field contains: {"webhook_url": "https://hooks.slack.com/services/..."}
        slack_url = slack_conn.extra_dejson.get("webhook_url")
        
        if not slack_url:
            logger.error(f"Slack Connection '{SLACK_CONN_ID}' is missing 'webhook_url' in 'Extra' field.")
            logger.error("Please set 'Extra' to: {\"webhook_url\": \"YOUR_SECRET_URL\"}")
            return  # Do not fail the callback, just log the error

        # --- 2. Get Task Info from Context ---
        ti = context.get("task_instance")
        dag_run = context.get("dag_run")
        
        dag_id = ti.dag_id
        task_id = ti.task_id
        log_url = ti.log_url
        run_id = dag_run.run_id if dag_run else "N/A"
        
        # Get the exception that caused the failure
        exception = context.get('exception')
        if isinstance(exception, AirflowException):
            error_message = str(exception)
        else:
            error_message = str(exception)
        
        # Clean up the message for Slack (removes newlines)
        error_message = error_message.replace("\n", " ").replace("\r", " ")

        # --- 3. Build Slack Message ---
        # Using Slack's 'mrkdwn' for formatting
        message = f"""
        :airflow-fail: *Airflow Task Failed*
        > *DAG:* `{dag_id}`
        > *Task:* `{task_id}`
        > *Run ID:* `{run_id}`
        > *Error:* ```{error_message}```
        > *<{log_url}|View Task Log>*
        """
        
        payload = {"text": message}
        headers = {"Content-type": "application/json"}

        # --- 4. Send Alert ---
        response = requests.post(slack_url, data=json.dumps(payload), headers=headers, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)

        logger.info(f"Successfully sent Slack alert for task: {task_id}")

    except Exception as e:
        # Log the error from the callback itself, but don't raise an exception
        # as this would obscure the original task failure.
        logger.error(f"ERROR: Failed to send Slack alert on task failure: {e}")
        logger.error(f"Original exception was: {context.get('exception')}")
# --- END OF SLACK FUNCTION ---


# ============================================================================
# Task 1: Validate Input
# ============================================================================

def task_validate_input(**context) -> Dict[str, Any]:
    """
    Task 1: Validate input configuration - SYNTAX ONLY.
    Builds config by merging user params over a set of defaults.
    """
    logger.info("=" * 80)
    logger.info("TASK 1: Validate Input Configuration")
    logger.info("=" * 80)

    params = context.get("params", {})
    logger.info(f"Received configuration from params: {json.dumps(params, indent=2)}")

    # --- EDIT: Start with a deep copy of the defaults ---
    config = copy.deepcopy(DEFAULT_CONFIG)
    
    # 1. Overwrite with Mandatory fields
    config["topic_name"] = params["topic_name"]
    config["partitions"] = params["partitions"]
    config["replication_factor"] = params["replication_factor"]
    config["bootstrap_servers"] = params["bootstrap_servers"]
    
    # 2. Overwrite 'monitoring' block (OpenTSDB is mandatory)
    config["monitoring"] = {
        "opentsdb_url": params["opentsdb_url"]
    }
    
    # 3. Add optional monitoring thresholds IF provided
    disk_thresholds_config = {}
    if params.get("disk_warning_percent") is not None:
        disk_thresholds_config["warning_percent"] = params["disk_warning_percent"]
    if params.get("disk_critical_gb") is not None:
        disk_thresholds_config["critical_gb"] = params["disk_critical_gb"]
    if disk_thresholds_config:
        config["monitoring"]["disk_thresholds"] = disk_thresholds_config

    # 4. Overwrite 'log_level' IF provided
    if params.get("log_level") is not None and params.get("log_level") != "":
        config["log_level"] = params["log_level"]
    
    # 5. Overwrite nested 'config' dict ONLY with user-provided optional values
    user_topic_configs = {}
    if params.get("retention_ms") is not None:
        user_topic_configs["retention.ms"] = params["retention_ms"]
    if params.get("retention_bytes") is not None:
        user_topic_configs["retention.bytes"] = params["retention_bytes"]
    if params.get("min_insync_replicas") is not None:
        user_topic_configs["min.insync.replicas"] = params["min_insync_replicas"]
    if params.get("segment_ms") is not None:
        user_topic_configs["segment.ms"] = params["segment_ms"]
    if params.get("segment_bytes") is not None:
        user_topic_configs["segment.bytes"] = params["segment_bytes"]
    if params.get("max_message_bytes") is not None:
        user_topic_configs["max.message.bytes"] = params["max_message_bytes"]
    if params.get("unclean_leader_election") is not None:
        user_topic_configs["unclean.leader.election.enable"] = params["unclean_leader_election"]
    if params.get("compression_type") is not None and params.get("compression_type") != "":
        user_topic_configs["compression.type"] = params["compression_type"]
    if params.get("cleanup_policy") is not None and params.get("cleanup_policy") != "":
        user_topic_configs["cleanup.policy"] = params["cleanup_policy"]

    # Merge the user-provided topic configs over the default topic configs
    if user_topic_configs:
        logger.info(f"User overrides for topic config: {json.dumps(user_topic_configs, indent=2)}")
        config["config"].update(user_topic_configs)
    
    # --- End of new logic ---

    # Log what configuration will be used
    logger.info("")
    logger.info("Final configuration to be executed (Defaults merged with User Input):")
    logger.info(f"  Topic Name: {config['topic_name']}")
    logger.info(f"  Partitions: {config['partitions']}")
    logger.info(f"  Replication Factor: {config['replication_factor']}")
    logger.info(f"  Bootstrap Servers: {config['bootstrap_servers']}")
    logger.info(f"  Log Level: {config.get('log_level', 'INFO')}")
    logger.info("  Topic Configs:")
    for k, v in config["config"].items():
        logger.info(f"    {k}: {v}")
    logger.info("  Monitoring Config:")
    for k, v in config["monitoring"].items():
        logger.info(f"    {k}: {v}")
    logger.info("")

    # --- Run Validations ---
    errors = []
    
    is_valid, error = validate_topic_name(config.get("topic_name", ""))
    if not is_valid: errors.append(f"Topic name: {error}")

    is_valid, error = validate_numeric_value(config.get("partitions", 0), "Partitions", min_val=1)
    if not is_valid: errors.append(error)

    is_valid, error = validate_numeric_value(config.get("replication_factor", 0), "Replication factor", min_val=1)
    if not is_valid: errors.append(error)

    bootstrap_servers = config.get("bootstrap_servers", [])
    if not bootstrap_servers or not isinstance(bootstrap_servers, list) or len(bootstrap_servers) == 0:
        errors.append("Bootstrap Servers are mandatory and must be a non-empty list.")
    
    if not config.get("monitoring", {}).get("opentsdb_url"):
        errors.append("OpenTSDB URL is mandatory.")

    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(error_msg)
        raise AirflowException(error_msg)

    logger.info("✓ All validations passed")
    context["task_instance"].xcom_push(key="validated_config", value=config)
    return config


# ============================================================================
# Task 2: Generate YAML
# ============================================================================

def task_generate_yaml(**context) -> str:
    """
    Task 2: Convert validated configuration to YAML string.
    """
    logger.info("=" * 80)
    logger.info("TASK 2: Generate YAML Configuration")
    logger.info("=" * 80)

    ti = context["task_instance"]
    config = ti.xcom_pull(task_ids="validate_input", key="validated_config")

    if not config:
        raise AirflowException("Failed to retrieve validated configuration")

    # Build YAML structure
    yaml_config = {
        "topic": {
            "name": config["topic_name"],
            "partitions": config["partitions"],
            "replication_factor": config["replication_factor"]
        },
        "cluster": {
            "bootstrap_servers": config["bootstrap_servers"]
        },
        "config": config["config"],
        "monitoring": config["monitoring"]
    }

    yaml_str = yaml.dump(yaml_config, default_flow_style=False, sort_keys=False)

    logger.info("Generated YAML configuration:")
    logger.info("-" * 80)
    logger.info(yaml_str)
    logger.info("-" * 80)

    context["task_instance"].xcom_push(key="yaml_content", value=yaml_str)
    context["task_instance"].xcom_push(key="log_level", value=config.get("log_level", "INFO"))

    return yaml_str


# ============================================================================
# Task 3 & 4: Reusable Salt Execution Function
# ============================================================================

def _execute_salt_task(dry_run: bool, **context) -> Dict[str, Any]:
    """
    Reusable function to execute a task on Salt.
    Maps `dry_run=True` to `--precheck` for this task.
    Fetches credentials from Airflow Connection 'salt_api_default'.
    """
    task_name = "Precheck" if dry_run else "Execute"
    
    logger.info("=" * 80)
    logger.info(f"TASK: Run Topic Creation ({task_name})")
    logger.info("=" * 80)

    ti = context["task_instance"]

    yaml_content = ti.xcom_pull(task_ids="generate_yaml", key="yaml_content")
    log_level = ti.xcom_pull(task_ids="generate_yaml", key="log_level")

    if not yaml_content:
        raise AirflowException("Failed to retrieve YAML configuration")

    # --- NEW: Fetch Salt Connection ---
    logger.info(f"Fetching Salt API credentials from connection: {SALT_CONN_ID}")
    try:
        salt_conn = BaseHook.get_connection(SALT_CONN_ID)
        
        # --- MODIFICATION: Manually build URL to fix InvalidSchema error ---
        # This forces the http:// prefix regardless of Conn Type
        api_url = f"http://{salt_conn.host}:{salt_conn.port}/run"
        # --- END MODIFICATION ---
        
        salt_api_username = salt_conn.login
        salt_api_password = salt_conn.password
        # Get eauth from 'Extras' field, default to 'pam'
        salt_eauth = salt_conn.extra_dejson.get("eauth", "pam") 

        if not (salt_conn.host and salt_api_username and salt_api_password):
            raise AirflowException("Connection is missing required fields (Host, Login, Password).")
            
    except Exception as e:
        logger.error(f"Failed to get Airflow Connection '{SALT_CONN_ID}': {e}")
        raise AirflowException(f"Failed to get Airflow Connection '{SALT_CONN_ID}': {e}")
    # --- END NEW ---

    logger.info(f"Task Type: {DAC_TASK_TYPE}")
    logger.info(f"Log Level: {log_level}")
    logger.info(f"Precheck Mode (dry_run): {dry_run}")

    headers = {"Content-Type": "application/json"}

    payload = {
        "username": salt_api_username,
        "password": salt_api_password,
        "eauth": salt_eauth,
        "client": "runner",
        "fun": "kafka_runner.run_dac_task",
        "kwarg": {
            "task_type": DAC_TASK_TYPE,
            "yaml_content": yaml_content,
            "log_level": log_level,
            "dry_run": dry_run
        }
    }

    logger.info(f"Calling Salt API: {api_url}")
    logger.info(f"Payload kwargs: {json.dumps(payload['kwarg'], indent=2)}")

    try:
        response = requests.post(
            api_url,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

        logger.info(f"Salt API Response Status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Salt API Error Response (Status {response.status_code}):")
            logger.error(response.text)
            raise AirflowException(f"Salt API request failed: HTTP {response.status_code}")

        result = response.json()
        logger.info("✓ Received response from Salt Master")
        
        logger.info(f"FULL SALT RESPONSE ({task_name}):")
        logger.info(json.dumps(result, indent=2))
        logger.info("=" * 80)

        if dry_run:
            context["task_instance"].xcom_push(key="salt_precheck_result", value=result)
        else:
            context["task_instance"].xcom_push(key="salt_execute_result", value=result)

        if dry_run:
            try:
                return_data = result["return"][0]
                script_output = return_data.get("stdout") or return_data.get("stderr")
                
                if not return_data.get("success", False):
                    error_msg = return_data.get("message", "Precheck failed")
                    logger.error(f"PRECHECK FAILED: {error_msg}")
                    if script_output:
                        print("\n--- Precheck Script Output (Failure) ---")
                        _log_multiline(script_output)
                        print("------------------------------------------\n")
                    # THIS EXCEPTION WILL TRIGGER THE SLACK ALERT
                    raise AirflowException(f"Topic pre-checks failed: {error_msg}")
                
                logger.info("✓ Topic pre-checks passed successfully.")
                if script_output:
                    print("\n--- Precheck Script Output ---")
                    _log_multiline(script_output)
                    print("------------------------------\n")

            except Exception as e:
                if not isinstance(e, AirflowException):
                    logger.error(f"Failed to parse precheck response: {e}")
                    raise AirflowException(f"Failed to parse precheck response: {str(e)}")
                else:
                    raise e # Re-raise the AirflowException from the precheck failure

        return result

    except requests.exceptions.Timeout:
        error_msg = f"Salt API request timed out after {REQUEST_TIMEOUT} seconds"
        logger.error(error_msg)
        raise AirflowException(error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = f"Salt API request failed: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)


# ============================================================================
# Task 5: Verify Result
# ============================================================================

def task_verify_result(**context) -> Dict[str, Any]:
    """
    Task 5: Verify the FINAL execution result from Salt.
    """
    logger.info("=" * 80)
    logger.info("TASK 5: Verify Execution Result")
    logger.info("=" * 80)

    ti = context["task_instance"]
    salt_result = ti.xcom_pull(task_ids="execute_creation", key="salt_execute_result")

    if not salt_result:
        raise AirflowException("Failed to retrieve Salt execution result")

    logger.info("Parsing Salt response...")

    try:
        if "return" not in salt_result or not salt_result["return"]:
             raise AirflowException("Invalid Salt API response format - missing 'return' key")

        master_result = salt_result["return"][0]
        
        if not isinstance(master_result, dict):
            raise AirflowException(f"Unexpected runner response format: {type(master_result)}")

        minion_used = master_result.get("minion_id", "unknown")
        logger.info(f"Response processed for minion: {minion_used}")

        verification_result = {
            "success": master_result.get("success", False),
            "message": master_result.get("message", "No message provided"),
            "minion_id": minion_used,
            "details": master_result.get("details", {}),
            "stdout": master_result.get("stdout", ""),
            "stderr": master_result.get("stderr", "")
        }
        
        script_output = verification_result.get("stdout") or verification_result.get("stderr")

        if verification_result["success"]:
            logger.info("=" * 80)
            logger.info("✓ SUCCESS: Topic creation completed")
            logger.info("=" * 80)
            logger.info(f"Message: {verification_result['message']}")
            logger.info(f"Executed on: {verification_result['minion_id']}")
            if script_output:
                print("\n--- Script Output ---")
                _log_multiline(script_output)
                print("---------------------\n")
        else:
            logger.error("=" * 80)
            logger.error("✗ FAILED: Topic creation failed")
            logger.error("=" * 80)
            logger.error(f"Message: {verification_result['message']}")
            logger.error(f"Executed on: {verification_result['minion_id']}")
            if script_output:
                print("\n--- Script Output (Failure) ---")
                _log_multiline(script_output)
                print("-------------------------------\n")
            
            # THIS EXCEPTION WILL TRIGGER THE SLACK ALERT
            raise AirflowException(f"Topic creation failed: {verification_result['message']}")

        context["task_instance"].xcom_push(key="verification_result", value=verification_result)
        return verification_result

    except Exception as e:
        logger.error(f"Unexpected error parsing Salt response: {e}")
        logger.error(f"Full Salt result: {json.dumps(salt_result, indent=2)}")
        if not isinstance(e, AirflowException):
            raise AirflowException(f"Failed to verify Salt execution: {str(e)}")
        else:
            raise e


# ============================================================================
# Task 6: Generate Report
# ============================================================================

# ============================================================================
# Task 6: Generate Report (AND SEND SUCCESS ALERT)
# ============================================================================

def task_generate_report(**context) -> None:
    """
    Task 6: Generate final execution report and send success alert.
    """
    logger.info("=" * 80)
    logger.info("TASK 6: Generate Final Report")
    logger.info("=" * 80)

    ti = context["task_instance"]

    validated_config = ti.xcom_pull(task_ids="validate_input", key="validated_config")
    precheck_result = ti.xcom_pull(task_ids="run_precheck", key="salt_precheck_result")
    verification_result = ti.xcom_pull(task_ids="verify_result", key="verification_result")

    if not validated_config:
        logger.warning("Could not retrieve validated_config for report.")
        # Even if config is missing, we might have a verification_result
        # that shows a failure, so we don't return here.
        validated_config = {} # Set to empty dict to avoid errors below

    if not verification_result:
        # This can happen if verify_result fails and is skipped
        logger.error("Could not retrieve verification_result. Task must have failed.")
        verification_result = {"success": False, "message": "Task failed before verification."}

    topic_name = validated_config.get("topic_name", "unknown")
    partitions = validated_config.get("partitions", 0)
    rf = validated_config.get("replication_factor", 0)

    # Parse Precheck Output
    precheck_output = "N/A"
    try:
        return_data = precheck_result["return"][0]
        precheck_output = return_data.get("stdout") or return_data.get("stderr", "Could not parse output")
    except Exception:
        pass 

    # Parse Final Output
    script_output = verification_result.get("stdout") or verification_result.get("stderr", "N/A")

    print("╔" + "=" * 78 + "╗")
    print("║" + " KAFKA TOPIC CREATION REPORT ".center(78) + "║")
    print("╚" + "=" * 78 + "╝")
    print("")
    print(f"  Topic Name:        {topic_name}")
    print(f"  Partitions:        {partitions}")
    print(f"  Replication Factor: {rf}")
    print(f"  Bootstrap Servers:  {validated_config.get('bootstrap_servers', [])}")
    print("")
    print("  Topic Configs Provided (others are defaults):")
    if validated_config.get("config"):
        for k, v in validated_config["config"].items():
            print(f"    {k}: {v}")
    else:
        print("    (Using all broker defaults)")
    
    print("")
    print("  Monitoring Config Provided:")
    if validated_config.get("monitoring"):
        for k, v in validated_config["monitoring"].items():
            print(f"    {k}: {v}")
    else:
        print("    (Validation Error, should be present)")
    print("")
    print("─" * 80)
    print("")
    print(f"  Status:            {'✓ SUCCESS' if verification_result['success'] else '✗ FAILED'}")
    print(f"  Message:            {verification_result['message']}")
    print(f"  Executed On:        {verification_result.get('minion_id', 'N/A')}")
    print("")
    
    print("  Precheck Output:")
    print("  " + "-" * 76)
    _log_multiline(precheck_output, prefix="  ")
    print("")

    if verification_result['success']:
        print("  Execution Output:")
        print("  " + "-" * 76)
        _log_multiline(script_output, prefix="  ")
        print("")

    print("═" * 80)
    print(f"Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 80)

    # --- NEW: Send Success Notification to Slack ---
    # This task runs on "all_done", so we MUST check if the previous task
    # (verify_result) actually succeeded.
    if verification_result and verification_result.get("success", False):
        try:
            logger.info("Sending success notification to Slack...")
            
            # 1. Get Slack Connection
            slack_conn = BaseHook.get_connection(SLACK_CONN_ID)
            slack_url = slack_conn.extra_dejson.get("webhook_url")
            
            if not slack_url:
                logger.warning(f"Cannot send Slack success alert. Connection '{SLACK_CONN_ID}' is missing 'webhook_url' in 'Extra' field.")
                return # Exit the function

            # 2. Build Message
            # (Get details from variables already pulled for the report)
            success_message = f"✅ *Kafka Topic Created Successfully*\n> *Topic:* `{topic_name}`\n> *Partitions:* {partitions}\n> *Replication Factor:* {rf}"
            
            payload = {"text": success_message}
            headers = {"Content-type": "application/json"}

            # 3. Send
            response = requests.post(
                slack_url, 
                data=json.dumps(payload), 
                headers=headers, 
                timeout=10
            )
            response.raise_for_status()
            logger.info("Successfully sent success alert to Slack.")

        except Exception as e:
            # Do not fail the task, just log a warning.
            logger.warning(f"Failed to send success Slack notification (task still succeeded): {e}")
    elif not verification_result:
        logger.warning("Skipping success alert: verification_result XCom not found.")
    else:
        logger.info("Skipping success alert: Task did not succeed.")
    # --- End of Slack Notification ---

# ============================================================================
# DAG Definition
# ============================================================================

default_args = {
    "owner": "kafka-admin",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _send_slack_alert_on_failure, # <-- ADDED FOR SLACK
}

dag = DAG(
    "kafka_topic_creation_via_salt",
    default_args=default_args,
    description="Automate Kafka topic creation via Salt-Stack (Modular)",
    schedule=None,
    catchup=False,
    tags=["kafka", "salt", "dac", "topic-creation", "modular"],
    params={
        # ==================== MANDATORY ====================
        # Salt params are now fetched from connection 'salt_api_default'
        "topic_name": Param(
            type="string",
            title="Topic Name (Mandatory)",
            description="Unique name for the Kafka topic (alphanumeric, '.', '_', '-' only)",
            minLength=1,
            maxLength=249,
            pattern="^[a-zA-Z0-9._-]+$",
        ),
        "partitions": Param(
            type="integer",
            title="Number of Partitions (Mandatory)",
            description="Number of partitions for the topic (must be > 0)",
            minimum=1,
        ),
        "replication_factor": Param(
            type="integer",
            title="Replication Factor (MandATORY)",
            description="Number of replicas per partition (typically 3 for production)",
            minimum=1,
        ),
        "bootstrap_servers": Param(
            type="array",
            title="Bootstrap Servers (Mandatory)",
            description="List of Kafka broker addresses (e.g., ['host1:6667', 'host2:6667']).",
        ),
        "opentsdb_url": Param(
            type="string",
            title="OpenTSDB URL (Mandatory)",
            description="URL for disk usage monitoring (e.g., http://opentsdb.example.com:4242).",
            minLength=1
        ),

        # ==================== OPTIONAL (Cluster) ====================
        "log_level": Param(
            default="",
            type=["string", "null"],
            title="Script Log Level (Optional)",
            description="Default: INFO. Override for more or less verbose script logs.",
            enum=["", "DEBUG", "INFO", "WARNING", "ERROR"],
        ),

        # ==================== OPTIONAL (Monitoring) ====================
        "disk_warning_percent": Param(
            default=None,
            type=["integer", "null"],
            title="Disk Warning % (Optional)",
            description="Default: (script default, e.g., 85). Warn if disk usage exceeds this.",
        ),
        "disk_critical_gb": Param(
            default=None,
            type=["integer", "null"],
            title="Disk Critical GB (Optional)",
            description="Default: (script default, e.g., 10). Warn if free space is less than this.",
        ),
        
        # ==================== OPTIONAL (Topic Configs) ====================
        "retention_ms": Param(
            default=None,
            type=["integer", "null"],
            title="Retention Time (ms) (Optional)",
            description="Default: (uses default config, e.g., 604800000).",
        ),
        "retention_bytes": Param(
            default=None,
            type=["integer", "null"],
            title="Retention Size (bytes) (Optional)",
            description="Default: (uses default config, e.g., 10737418240).",
        ),
        "min_insync_replicas": Param(
            default=None,
            type=["integer", "null"],
            title="Min In-Sync Replicas (Optional)",
            description="Default: (uses default config, e.g., 2).",
        ),
        "compression_type": Param(
            default="",
            type=["string", "null"],
            title="Compression Type (Optional)",
            description="Default: (uses default config, e.g., producer)",
            enum=["", "uncompressed", "gzip", "snappy", "lz4", "zstd", "producer"],
        ),
        "cleanup_policy": Param(
            default="",
            type=["string", "null"],
            title="Cleanup Policy (Optional)",
            description="Default: (uses default config, e.g., delete)",
            enum=["", "delete", "compact", "delete,compact"],
        ),
        "segment_ms": Param(
            default=None,
            type=["integer", "null"],
            title="Segment Roll Time (ms) (Optional)",
            description="Default: (uses default config, e.g., 604800000).",
        ),
        "segment_bytes": Param(
            default=None,
            type=["integer", "null"],
            title="Segment Size (bytes) (Optional)",
            description="Default: (uses default config, e.g., 1073741824).",
        ),
        "max_message_bytes": Param(
            default=None,
            type=["integer", "null"],
            title="Max Message Size (bytes) (Optional)",
            description="Default: (uses default config, e.g., 1048576).",
        ),
        "unclean_leader_election": Param(
            default=None,
            type=["boolean", "null"],
            title="Unclean Leader Election (Optional)",
            description="Default: (uses default config, e.g., False).",
        ),
    },
)

# --- Define tasks with clear names ---
validate_input = PythonOperator(
    task_id="validate_input",
    python_callable=task_validate_input,
    dag=dag,
)

generate_yaml = PythonOperator(
    task_id="generate_yaml",
    python_callable=task_generate_yaml,
    dag=dag,
)

run_precheck = PythonOperator(
    task_id="run_precheck",
    python_callable=_execute_salt_task,
    op_kwargs={"dry_run": True},
    dag=dag,
)

execute_creation = PythonOperator(
    task_id="execute_creation",
    python_callable=_execute_salt_task,
    op_kwargs={"dry_run": False},
    dag=dag,
)

verify_result = PythonOperator(
    task_id="verify_result",
    python_callable=task_verify_result,
    dag=dag,
)

generate_report = PythonOperator(
    task_id="generate_report",
    python_callable=task_generate_report,
    trigger_rule="all_done",
    dag=dag,
)

# --- Define task dependencies ---
validate_input >> generate_yaml >> run_precheck >> execute_creation >> verify_result >> generate_report
