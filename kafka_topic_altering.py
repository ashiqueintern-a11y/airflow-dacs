"""
Airflow DAG for Kafka Topic Alteration via Salt-Stack
=====================================================
Restructured with clear separation of concerns and a dry-run step.

Tasks:
1. validate_input - Validates user configuration
2. generate_yaml - Converts config to YAML string for the alteration script
3. precheck - Executes the alteration script in --dry-run mode
4. execute_alteration - Performs the actual execution via Salt
5. verify_result - Verifies the final execution result
6. generate_report - Creates final report (includes dry-run output)

Author: DevOps Team
Version: 3.3.1 (Renamed dry-run task)
Compatible with: Airflow 3.0+, Salt 3000+, Kafka 2.8.2

Note: This DAG calls the 'topic_alteration' task in the Salt Runner.
Note: kafka_home and log_dirs removed from params - script auto-detects these.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Param
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import json
import yaml
import logging
import re
from typing import Dict, Any, Optional, Tuple

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Constants
# ============================================================================

SALT_MASTER_URL = Variable.get("salt_master_url", default_var="http://stg-hdpashique105.phonepe.nb6:8000")
SALT_API_USERNAME = Variable.get("salt_api_username", default_var="saltapi")
SALT_API_PASSWORD = Variable.get("salt_api_password", default_var="password")
SALT_EAUTH = Variable.get("salt_eauth", default_var="pam")
SALT_TARGET_MINION = Variable.get("salt_target_minion", default_var="role:kafka_broker")
SALT_TARGET_TYPE = Variable.get("salt_target_type", default_var="grain")

# --- MODIFIED FOR ALTERATION ---
DAC_TASK_TYPE = "topic_alteration"
# --- END MODIFICATION ---

REQUEST_TIMEOUT = 300

# Default example config (defaults are set in Params)
DEFAULT_CONFIG = {
    "topic_name": "example.events.prod",
    "bootstrap_servers": ["stg-hdpashique101:6667", "stg-hdpashique102:6667", "stg-hdpashique103:6667"],
    "log_level": "INFO",
    "alteration": {
        "partitions": {
            "target": None
        },
        "retention": {
            "new": None
        },
        "other_configs": {}
    }
}

# ============================================================================
# Helper Functions
# ============================================================================

def _log_multiline(output_string: str, prefix: str = "  > "):
    """
    Helper to print a multiline string for readable Airflow logs.
    Uses print() to avoid double-logging prefixes and
    strips the script's own log prefix for clarity.
    """
    if not output_string:
        print(f"{prefix}(No script output received)")
        return

    lines = output_string.strip().split('\n')
    for line in lines:
        line_to_print = line.strip()
        if not line_to_print:  # Skip empty lines
            continue

        # Try to split the line by the script's log separator ' - '
        parts = line_to_print.split(' - ', 2)

        # Check if it looks like a log line (e.g., timestamp, level, message)
        if len(parts) == 3:
            # It's a log line, just print the message part (parts[2])
            message = parts[2].strip()
            if message: # Don't print empty log messages
                print(f"{prefix}{message}")
        else:
            # It's not a standard log line (e.g., '====' or '[DRY-RUN]...'), print as is
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


# ============================================================================
# Task 1: Validate Input (Modified for Alteration)
# ============================================================================

def task_validate_input(**context) -> Dict[str, Any]:
    """
    Task 1: Validate input configuration for topic alteration.
    """
    logger.info("=" * 80)
    logger.info("TASK 1: Validate Input Configuration (Alteration)")
    logger.info("=" * 80)

    params = context.get("params", {})
    logger.info(f"Received configuration from params: {json.dumps(params, indent=2)}")


    # Initialize config
    config = {
        "topic_name": params.get("topic_name"),
        "bootstrap_servers": params.get("bootstrap_servers"),
        "log_level": params.get("log_level", "INFO"),
        "alteration": {
            "partitions": {},
            "retention": {},
            "other_configs": {}
        }
    }

    # --- Build Alteration Config from Params ---

    if params.get("target_partitions"):
        config["alteration"]["partitions"]["target"] = params["target_partitions"]

    # Check for None and empty string for retention
    new_retention = params.get("new_retention")
    if new_retention is not None and new_retention != "":
        config["alteration"]["retention"]["new"] = new_retention

    # Collect other_configs from individual params
    other_configs = {}
    config_keys = [
        "cleanup.policy", "compression.type", "max.message.bytes",
        "min.insync.replicas", "segment.bytes", "segment.ms"
    ]

    # Update loop to check for None and empty string
    for key in config_keys:
        # Use the param name (e.g., "cleanup_policy") to find the value
        param_key = key.replace('.', '_')
        param_value = params.get(param_key)
        # Check for both None and empty string
        if param_value is not None and param_value != "":
            other_configs[key] = param_value

    if other_configs:
        config["alteration"]["other_configs"] = other_configs

    # --- Run Validations ---
    errors = []

    # 1. Topic name
    is_valid, error = validate_topic_name(config.get("topic_name", ""))
    if not is_valid:
        errors.append(f"Topic name: {error}")

    # 2. Bootstrap servers
    bootstrap_servers = config.get("bootstrap_servers", [])
    if not bootstrap_servers or not isinstance(bootstrap_servers, list) or len(bootstrap_servers) == 0:
        errors.append("Bootstrap servers must be a non-empty list (at least 1 required)")
    else:
        for server in bootstrap_servers:
            if ':' not in str(server):
                errors.append(f"Invalid bootstrap server format: {server} (expected host:port)")

    # 3. Log level
    log_level = config.get("log_level", "INFO")
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        errors.append(f"Invalid log level: {log_level}. Must be one of: DEBUG, INFO, WARNING, ERROR")

    # 4. Check if at least one alteration is specified
    has_partition_change = "target" in config["alteration"]["partitions"]
    has_retention_change = "new" in config["alteration"]["retention"]
    has_other_configs = bool(config["alteration"]["other_configs"])

    if not has_partition_change and not has_retention_change and not has_other_configs:
        errors.append("No alterations specified. You must provide a target_partitions, new_retention, or at least one other config to change.")

    # 5. Validate target_partitions if provided
    if has_partition_change:
        is_valid, error = validate_numeric_value(
            config["alteration"]["partitions"]["target"], "Target partitions", min_val=1
        )
        if not is_valid:
            errors.append(error)

    # If any errors, raise exception
    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(error_msg)
        raise AirflowException(error_msg)

    logger.info("✓ All syntax validations passed")
    logger.info(f"Topic: {config['topic_name']}")
    logger.info(f"Target Partitions: {config['alteration']['partitions'].get('target', 'N/A')}")
    logger.info(f"New Retention: {config['alteration']['retention'].get('new', 'N/A')}")
    logger.info(f"Other Configs: {json.dumps(config['alteration']['other_configs'])}")
    logger.info("")
    logger.info("Note: Business logic validations (topic existence, etc.)")
    logger.info("      will be performed by the script during the dry-run.")

    # Push validated config to XCom
    context["task_instance"].xcom_push(key="validated_config", value=config)

    return {
        "status": "success",
        "topic_name": config["topic_name"],
        "alterations_pending": True
    }


# ============================================================================
# Task 2: Generate YAML (Modified for Alteration)
# ============================================================================

def task_generate_yaml(**context) -> str:
    """
    Task 2: Convert validated configuration to the YAML string
    expected by the topic_alteration.py script.
    """
    logger.info("=" * 80)
    logger.info("TASK 2: Generate YAML Configuration (Alteration)")
    logger.info("=" * 80)

    ti = context["task_instance"]
    config = ti.xcom_pull(task_ids="validate_input", key="validated_config")

    if not config:
        raise AirflowException("Failed to retrieve validated configuration")

    # Build YAML structure expected by alteration script
    yaml_config = {
        "topic": {
            "name": config["topic_name"],
            "bootstrap_servers": config["bootstrap_servers"]
        },
        "alteration": {}
    }

    # Only add sections if they have content
    if config["alteration"]["partitions"]:
        yaml_config["alteration"]["partitions"] = config["alteration"]["partitions"]

    if config["alteration"]["retention"]:
        yaml_config["alteration"]["retention"] = config["alteration"]["retention"]

    if config["alteration"]["other_configs"]:
        yaml_config["alteration"]["other_configs"] = config["alteration"]["other_configs"]

    # Convert to YAML string
    yaml_str = yaml.dump(yaml_config, default_flow_style=False, sort_keys=False)

    logger.info("Generated YAML configuration:")
    logger.info("-" * 80)
    logger.info(yaml_str)
    logger.info("-" * 80)

    # Push YAML and log level to XCom
    context["task_instance"].xcom_push(key="yaml_content", value=yaml_str)
    context["task_instance"].xcom_push(key="log_level", value=config.get("log_level", "INFO"))

    return yaml_str


# ============================================================================
# Task 3 & 4: Execute on Salt (Modified for Dry-Run and Execute)
# ============================================================================

def _execute_salt_task(dry_run: bool, **context) -> Dict[str, Any]:
    """
    Reusable function to execute a task on Salt.
    Can be run in dry-run or execute mode.
    """
    task_name = "Precheck" if dry_run else "Execute" # Changed "Dry-Run" to "Precheck"

    logger.info("=" * 80)
    logger.info(f"TASK: Execute on Salt Master ({task_name})")
    logger.info("=" * 80)

    ti = context["task_instance"]

    # Get YAML content and parameters from previous tasks
    yaml_content = ti.xcom_pull(task_ids="generate_yaml", key="yaml_content")
    log_level = ti.xcom_pull(task_ids="generate_yaml", key="log_level")

    if not yaml_content:
        raise AirflowException("Failed to retrieve YAML configuration")

    logger.info(f"Task Type: {DAC_TASK_TYPE}")
    logger.info(f"Log Level: {log_level}")
    logger.info(f"Precheck Mode (dry_run): {dry_run}") # Renamed log

    # Build Salt API payload
    api_url = f"{SALT_MASTER_URL}/run"
    headers = {"Content-Type": "application/json"}

    payload = {
        "username": SALT_API_USERNAME,
        "password": SALT_API_PASSWORD,
        "eauth": SALT_EAUTH,
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
    logger.info(f"Payload function: {payload['fun']}")
    logger.info(f"Payload kwargs: {payload['kwarg']}")

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

        response.raise_for_status()
        result = response.json()

        logger.info("✓ Received response from Salt Master")

        # We log the full JSON *first* for debugging, then the readable output.
        logger.info(f"FULL SALT RESPONSE ({task_name}):")
        logger.info(json.dumps(result, indent=2))
        logger.info("=" * 80)

        # Push result to a unique XCom key based on the mode
        if dry_run:
            context["task_instance"].xcom_push(key="salt_precheck_result", value=result) # Renamed key
        else:
            context["task_instance"].xcom_push(key="salt_execute_result", value=result)

        # Check precheck success
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
                        print("----------------------------------------\n")

                    raise AirflowException(f"Pre-checks failed: {error_msg}")

                logger.info("✓ Pre-checks passed successfully.")
                if script_output:
                    print("\n--- Precheck Script Output ---")
                    _log_multiline(script_output)
                    print("----------------------------\n")

            except Exception as e:
                # Catch if we fail to parse the response
                if not isinstance(e, AirflowException): # Don't re-wrap our own exception
                    logger.error(f"Failed to parse precheck response: {e}")
                    logger.error(f"Full response: {json.dumps(result, indent=2)}")
                    raise AirflowException(f"Failed to parse precheck response: {str(e)}")
                else:
                    raise e # Re-raise the AirflowException

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
# Task 5: Verify Result (Modified for Readability)
# ============================================================================

def task_verify_result(**context) -> Dict[str, Any]:
    """
    Task 5: Verify the FINAL execution result from Salt.
    """
    logger.info("=" * 80)
    logger.info("TASK 5: Verify Execution Result")
    logger.info("=" * 80)

    ti = context["task_instance"]
    salt_result = ti.xcom_pull(task_ids="execute_alteration", key="salt_execute_result")

    if not salt_result:
        raise AirflowException("Failed to retrieve Salt execution result")

    logger.info("Parsing final execution Salt response...")

    try:
        if "return" not in salt_result or not salt_result["return"]:
            raise AirflowException("Invalid Salt API response format - missing 'return' key or empty")

        return_data = salt_result["return"][0]

        if not isinstance(return_data, dict) or not return_data:
            raise AirflowException(f"Unexpected return data format: {type(return_data)}")

        master_result = return_data
        minion_id = master_result.get("minion_id", "unknown")
        logger.info(f"Response processed for minion: {minion_id}")

        verification_result = {
            "success": master_result.get("success", False),
            "message": master_result.get("message", "No message provided"),
            "minion_id": minion_id,
            "details": master_result.get("details", {}),
            "stdout": master_result.get("stdout", ""),
            "stderr": master_result.get("stderr", "")
        }

        script_output = verification_result.get("stdout") or verification_result.get("stderr")

        if verification_result["success"]:
            logger.info("=" * 80)
            logger.info("✓ SUCCESS: Topic alteration completed")
            logger.info("=" * 80)
            logger.info(f"Message: {verification_result['message']}")

            if script_output:
                print("\n--- Final Execution Script Output ---")
                _log_multiline(script_output)
                print("-------------------------------------\n")

        else:
            logger.error("=" * 80)
            logger.error("✗ FAILED: Topic alteration failed")
            logger.error("=" * 80)
            logger.error(f"Message: {verification_result['message']}")

            if script_output:
                print("\n--- Final Execution Script Output (Failure) ---")
                _log_multiline(script_output)
                print("-------------------------------------------------\n")

            raise AirflowException(f"Topic alteration failed: {verification_result['message']}")

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
# Task 6: Generate Report (Modified for Readability)
# ============================================================================

def task_generate_report(**context) -> None:
    """
    Task 6: Generate final execution report.
    Consolidates info from validation, precheck, and execution.
    """
    logger.info("=" * 80)
    logger.info("TASK 6: Generate Final Report")
    logger.info("=" * 80)

    ti = context["task_instance"]

    # Gather all data
    validated_config = ti.xcom_pull(task_ids="validate_input", key="validated_config")
    precheck_salt_result = ti.xcom_pull(task_ids="precheck", key="salt_precheck_result") # Renamed task_id
    verification_result = ti.xcom_pull(task_ids="verify_result", key="verification_result")

    if not validated_config:
        logger.warning("Could not retrieve validated config for report.")
        return

    topic_name = validated_config.get("topic_name", "unknown")

    # --- Parse Precheck Output ---
    precheck_output = "N/A"
    try:
        return_data = precheck_salt_result["return"][0]
        precheck_output = return_data.get("stdout") or return_data.get("stderr", "Could not parse output")
    except Exception:
        pass

    # --- Parse Final Output ---
    status = "✓ SUCCESS"
    message = "Operation completed successfully."
    final_output = "N/A"

    if verification_result:
        status = "✓ SUCCESS"
        message = verification_result.get("message", "Success")
        final_output = verification_result.get("stdout") or verification_result.get("stderr", "N/A")
    else:
        status = "✗ FAILED"
        message = "Execution failed. Check 'verify_result' task logs for details."
        final_output = precheck_output # Use precheck as last known output

    # --- Use print() for readable report ---
    print("╔" + "=" * 78 + "╗")
    print("║" + " KAFKA TOPIC ALTERATION REPORT ".center(78) + "║")
    print("╚" + "=" * 78 + "╝")
    print("")
    print(f"  Topic Name: {topic_name}")
    print(f"  Bootstrap Servers: {len(validated_config.get('bootstrap_servers', []))}")
    print("")
    print("  Alterations Requested:")

    alt = validated_config.get("alteration", {})
    if "target" in alt.get("partitions", {}):
        print(f"    - Increase Partitions to: {alt['partitions']['target']}")
    if "new" in alt.get("retention", {}):
        print(f"    - Change Retention to: {alt['retention']['new']}")
    if alt.get("other_configs"):
        print("    - Change Other Configs:")
        for k, v in alt["other_configs"].items():
            print(f"      - {k}: {v}")

    print("─" * 80)
    print("")
    print(f"  Final Status:  {status}")
    print(f"  Final Message: {message}")
    print("")

    print("  Precheck Output:")
    print("  " + "-" * 76)
    _log_multiline(precheck_output, prefix="  ")
    print("")

    if status == "✓ SUCCESS":
        print("  Final Execution Output:")
        print("  " + "-" * 76)
        _log_multiline(final_output, prefix="  ")
        print("")

    print("═" * 80)
    print(f"Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 80)


# ============================================================================
# DAG Definition (Modified for Alteration)
# ============================================================================

default_args = {
    "owner": "kafka-admin",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "kafka_topic_alteration_via_salt",  # New DAG ID
    default_args=default_args,
    description="Automate Kafka topic alteration via Salt-Stack (with dry-run)",
    schedule=None,
    catchup=False,
    tags=["kafka", "salt", "dac", "topic-alteration", "modular"],

    params={
        # ==================== Topic Identification ====================
        "topic_name": Param(
            type="string",
            title="Topic Name",
            description="Name of the existing topic to alter",
            minLength=1,
            maxLength=249,
            pattern="^[a-zA-Z0-9._-]+$",
        ),
        "bootstrap_servers": Param(
            default=["stg-hdpashique101.phonepe.nb6:6667"],
            type="array",
            title="Bootstrap Servers",
            description="List of Kafka broker addresses ('host:port')",
        ),
        "log_level": Param(
            default="INFO",
            type="string",
            title="Script Log Level",
            enum=["DEBUG", "INFO", "WARNING", "ERROR"],
        ),

        # ==================== Alteration: Partitions ====================
        "target_partitions": Param(
            default=None,
            type=["integer", "null"],
            title="Target Partitions (Optional)",
            description="New total partition count. (e.g., 12). Must be > current.",
            minimum=1,
        ),

        # ==================== Alteration: Retention ====================
        "new_retention": Param(
            default="",
            type=["string", "null"],
            title="New Retention (Optional)",
            description="New retention period (e.g., '336h', '14d', '604800000ms')"
        ),

        # ==================== Alteration: Other Configs ====================
        "cleanup_policy": Param(
            default="",
            type=["string", "null"],
            title="Cleanup Policy (Optional)",
            description="Set new cleanup policy",
            enum=["", "delete", "compact", "delete,compact"],
        ),
        "compression_type": Param(
            default="",
            type=["string", "null"],
            title="Compression Type (Optional)",
            description="Set new compression type",
            enum=["", "uncompressed", "gzip", "snappy", "lz4", "zstd", "producer"],
        ),
        "max_message_bytes": Param(
            default=None,
            type=["integer", "null"],
            title="Max Message Size (bytes) (Optional)",
            description="Set new max message size (e.g., 5242880 for 5MB)",
            minimum=1024,
        ),
        "min_insync_replicas": Param(
            default=None,
            type=["integer", "null"],
            title="Min In-Sync Replicas (Optional)",
            description="Set new min in-sync replicas (e.g., 2)",
            minimum=1,
        ),
        "segment_bytes": Param(
            default=None,
            type=["integer", "null"],
            title="Segment Size (bytes) (Optional)",
            description="Set new segment size (e.g., 1073741824 for 1GB)",
            minimum=1048576,
        ),
        "segment_ms": Param(
            default=None,
            type=["integer", "null"],
            title="Segment Roll Time (ms) (Optional)",
            description="Set new segment roll time (e.g., 604800000 for 7 days)",
            minimum=60000,
        ),
    },
)

# --- Define tasks ---

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

# --- EDIT START: Renamed task ---
precheck = PythonOperator(
    task_id="precheck",
    python_callable=_execute_salt_task,
    op_kwargs={"dry_run": True},
    dag=dag,
)
# --- EDIT END ---

execute_alteration = PythonOperator(
    task_id="execute_alteration",
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
validate_input >> generate_yaml >> precheck >> execute_alteration >> verify_result >> generate_report
