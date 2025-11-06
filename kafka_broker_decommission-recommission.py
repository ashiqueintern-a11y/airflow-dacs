"""
Airflow DAG for Kafka Node Rotation (Decommission/Recommission)
================================================================
Orchestrates broker decommission or recommission via Salt-Stack.

Tasks:
1. validate_input - Validates user inputs (Broker ID/Hostname, Action, Config)
2. generate_yaml - Converts config to YAML string for the script
3. dry_run_action - Executes the script in --dry-run mode
4. execute_action - Performs the actual decommission/recommission via Salt
5. verify_result - Verifies the final execution result
6. generate_report - Creates final report

Author: DevOps Team
Version: 1.2.0 (Accepts Broker ID or Hostname)
Compatible with: Airflow 3.0+, Salt 3000+, Kafka 2.8.2
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

# --- MODIFIED FOR NODE ROTATION ---
DAC_TASK_TYPE = "node_rotation"
# --- END MODIFICATION ---

REQUEST_TIMEOUT = 900  # Increased to 15 mins for long-running (re)decommissions

# ============================================================================
# Helper Functions (for log readability)
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

# ============================================================================
# Task 1: Validate Input
# ============================================================================

def task_validate_input(**context) -> Dict[str, Any]:
    """
    Task 1: Validate input configuration for node rotation.
    """
    logger.info("=" * 80)
    logger.info("TASK 1: Validate Input Configuration (Node Rotation)")
    logger.info("=" * 80)

    params = context.get("params", {})
    logger.info(f"Received configuration from params: {json.dumps(params, indent=2)}")

    errors = []
    
    # 1. Validate Broker ID or Hostname
    # --- EDIT START: Changed broker_id to broker_target ---
    broker_target = params.get("broker_target")
    if not broker_target:
        errors.append("Broker ID or Hostname is required.")
    # --- EDIT END: Removed the regex check for numbers ---

    # 2. Validate Action
    action = params.get("action")
    # --- EDIT START: Removed 'rollback' ---
    if not action or action not in ["decommission", "recommission"]:
        errors.append(f"Invalid action. Must be one of: decommission, recommission.")
    # --- EDIT END ---

    # 3. Validate Bootstrap Servers
    bootstrap_servers = params.get("bootstrap_servers")
    if not bootstrap_servers or ':' not in bootstrap_servers:
        errors.append("Bootstrap Servers are required and must be in 'host:port' or 'host1:port,host2:port' format.")

    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(error_msg)
        raise AirflowException(error_msg)

    logger.info("✓ All syntax validations passed")

    validated_config = {
        "yaml_config": {},
        "runner_kwargs": {
            # --- EDIT START: Pass the target to the 'broker_id' kwarg ---
            # The Salt runner function 'kafka_runner.run_dac_task'
            # still expects the 'broker_id' kwarg, but now we pass
            # it either an ID or a hostname.
            "broker_id": str(broker_target),
            # --- EDIT END ---
            "action": action,
            "log_level": params.get("log_level", "INFO")
        }
    }

    # Build the YAML config dictionary
    yaml_config = validated_config["yaml_config"]
    yaml_config["bootstrap_servers"] = bootstrap_servers

    # Add optional params to YAML only if they are set
    if params.get("opentsdb_url"):
        yaml_config["opentsdb_url"] = params["opentsdb_url"]
    if params.get("state_directory"):
        yaml_config["state_directory"] = params["state_directory"]
    if params.get("cpu_threshold"):
        yaml_config["cpu_threshold"] = params["cpu_threshold"]
    if params.get("disk_threshold"):
        yaml_config["disk_threshold"] = params["disk_threshold"]
    
    # --- EDIT START: Updated log message ---
    logger.info(f"Action: {action.upper()} on Broker: {broker_target}")
    # --- EDIT END ---
    
    context["task_instance"].xcom_push(key="validated_config", value=validated_config)
    
    return validated_config


# ============================================================================
# Task 2: Generate YAML
# ============================================================================

def task_generate_yaml(**context) -> str:
    """
    Task 2: Convert validated config to YAML string and pass runner args.
    """
    logger.info("=" * 80)
    logger.info("TASK 2: Generate YAML Configuration")
    logger.info("=" * 80)
    
    ti = context["task_instance"]
    validated_config = ti.xcom_pull(task_ids="validate_input", key="validated_config")
    
    if not validated_config:
        raise AirflowException("Failed to retrieve validated configuration")

    yaml_to_generate = validated_config.get("yaml_config", {})
    runner_kwargs = validated_config.get("runner_kwargs", {})

    # Convert to YAML string
    yaml_str = yaml.dump(yaml_to_generate, default_flow_style=False, sort_keys=False)
    
    logger.info("Generated YAML configuration:")
    logger.info("-" * 80)
    logger.info(yaml_str)
    logger.info("-" * 80)
    logger.info(f"Runner arguments: {json.dumps(runner_kwargs, indent=2)}")
    
    context["task_instance"].xcom_push(key="yaml_content", value=yaml_str)
    context["task_instance"].xcom_push(key="runner_kwargs", value=runner_kwargs)
    
    return yaml_str


# ============================================================================
# Task 3 & 4: Execute on Salt
# ============================================================================

def _execute_salt_task(dry_run: bool, **context) -> Dict[str, Any]:
    """
    Reusable function to execute a task on Salt.
    """
    task_name = "Dry-Run" if dry_run else "Execute"
    logger.info("=" * 80)
    logger.info(f"TASK: Execute on Salt Master ({task_name})")
    logger.info("=" * 80)

    ti = context["task_instance"]
    
    yaml_content = ti.xcom_pull(task_ids="generate_yaml", key="yaml_content")
    runner_kwargs = ti.xcom_pull(task_ids="generate_yaml", key="runner_kwargs")
    
    if not yaml_content or not runner_kwargs:
        raise AirflowException("Failed to retrieve YAML content or runner kwargs")
    
    logger.info(f"Task Type: {DAC_TASK_TYPE}")
    logger.info(f"Dry Run Mode: {dry_run}")
    logger.info(f"Runner Kwargs: {runner_kwargs}")

    # Build Salt API payload
    api_url = f"{SALT_MASTER_URL}/run"
    headers = {"Content-Type": "application/json"}

    payload_kwargs = {
        "task_type": DAC_TASK_TYPE,
        "yaml_content": yaml_content,
        "dry_run": dry_run
    }
    
    payload_kwargs.update(runner_kwargs)
    
    payload = {
        "username": SALT_API_USERNAME,
        "password": SALT_API_PASSWORD,
        "eauth": SALT_EAUTH,
        "client": "runner",
        "fun": "kafka_runner.run_dac_task",
        "kwarg": payload_kwargs
    }

    logger.info(f"Calling Salt API: {api_url}")
    logger.info(f"Payload function: {payload['fun']}")
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

        # Push result to a unique XCom key
        if dry_run:
            context["task_instance"].xcom_push(key="salt_dry_run_result", value=result)
        else:
            context["task_instance"].xcom_push(key="salt_execute_result", value=result)

        # Check dry-run success
        if dry_run:
            try:
                return_data = result["return"][0]
                script_output = return_data.get("stdout") or return_data.get("stderr")
                
                if not return_data.get("success", False):
                    error_msg = return_data.get("message", "Dry-run failed")
                    logger.error(f"DRY-RUN FAILED: {error_msg}")
                    if script_output:
                        print("\n--- Dry-Run Script Output (Failure) ---")
                        _log_multiline(script_output)
                        print("-----------------------------------------\n")
                    raise AirflowException(f"Dry-run pre-checks failed: {error_msg}")
                
                logger.info("✓ Dry-run pre-checks passed successfully.")
                if script_output:
                    print("\n--- Dry-Run Script Output ---")
                    _log_multiline(script_output)
                    print("-----------------------------\n")

            except Exception as e:
                if not isinstance(e, AirflowException):
                    logger.error(f"Failed to parse dry-run response: {e}")
                    raise AirflowException(f"Failed to parse dry-run response: {str(e)}")
                else:
                    raise e

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
    salt_result = ti.xcom_pull(task_ids="execute_action", key="salt_execute_result")

    if not salt_result:
        raise AirflowException("Failed to retrieve Salt execution result")

    logger.info("Parsing final execution Salt response...")

    try:
        if "return" not in salt_result or not salt_result["return"]:
            raise AirflowException("Invalid Salt API response format - missing 'return' key")
        
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
            logger.info(f"✓ SUCCESS: {verification_result['message']}")
            logger.info("=" * 80)
            if script_output:
                print("\n--- Final Execution Script Output ---")
                _log_multiline(script_output)
                print("-------------------------------------\n")
        else:
            logger.error("=" * 80)
            logger.error(f"✗ FAILED: {verification_result['message']}")
            logger.error("=" * 80)
            if script_output:
                print("\n--- Final Execution Script Output (Failure) ---")
                _log_multiline(script_output)
                print("-------------------------------------------------\n")
            raise AirflowException(f"Node rotation failed: {verification_result['message']}")
        
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

def task_generate_report(**context) -> None:
    """
    Task 6: Generate final execution report.
    """
    logger.info("=" * 80)
    logger.info("TASK 6: Generate Final Report")
    logger.info("=" * 80)

    ti = context["task_instance"]
    
    validated_config = ti.xcom_pull(task_ids="validate_input", key="validated_config")
    dry_run_salt_result = ti.xcom_pull(task_ids="dry_run_action", key="salt_dry_run_result")
    verification_result = ti.xcom_pull(task_ids="verify_result", key="verification_result")

    if not validated_config:
        logger.warning("Could not retrieve validated config for report.")
        return

    runner_kwargs = validated_config.get("runner_kwargs", {})
    yaml_config = validated_config.get("yaml_config", {})

    # Parse Dry-Run Output
    dry_run_output = "N/A"
    try:
        return_data = dry_run_salt_result["return"][0]
        dry_run_output = return_data.get("stdout") or return_data.get("stderr", "Could not parse output")
    except Exception:
        pass 

    # Parse Final Output
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
        final_output = dry_run_output

    # --- Use print() for readable report ---
    print("╔" + "=" * 78 + "╗")
    print("║" + " KAFKA NODE ROTATION REPORT ".center(78) + "║")
    print("╚" + "=" * 78 + "╝")
    print("")
    print("  Operation Details:")
    # --- EDIT START: Updated label ---
    print(f"    - Broker: {runner_kwargs.get('broker_id', 'N/A')}")
    # --- EDIT END ---
    print(f"    - Action:   {runner_kwargs.get('action', 'N/A').upper()}")
    print("")
    print("  Configuration:")
    print(f"    - bootstrap_servers: {yaml_config.get('bootstrap_servers', 'N/A')}")
    if yaml_config.get('opentsdb_url'):
        print(f"    - opentsdb_url: {yaml_config['opentsdb_url']}")
    if yaml_config.get('state_directory'):
        print(f"    - state_directory: {yaml_config['state_directory']}")

    print("─" * 80)
    print("")
    print(f"  Final Status:  {status}")
    print(f"  Final Message: {message}")
    print("")

    print("  Dry-Run Output:")
    print("  " + "-" * 76)
    _log_multiline(dry_run_output, prefix="  ")
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
}

dag = DAG(
    "kafka_node_rotation_via_salt",
    default_args=default_args,
    description="Decommission or Recommission a Kafka broker via Salt-Stack",
    schedule=None,
    catchup=False,
    tags=["kafka", "salt", "dac", "node-rotation", "decommission"],

    params={
        # ==================== Main Action ====================
        # --- EDIT START: Changed broker_id to broker_target ---
        "broker_target": Param(
            type="string",
            title="Broker ID or Hostname",
            description="The numeric ID (e.g., 1001) or FQDN (e.g., host.example.com) of the broker.",
        ),
        # --- EDIT END ---
        "action": Param(
            default="decommission",
            type="string",
            title="Action",
            description="The action to perform on the broker.",
            # --- EDIT START: Removed 'rollback' ---
            enum=["decommission", "recommission"],
            # --- EDIT END ---
        ),
        
        # ==================== Required Config ====================
        "bootstrap_servers": Param(
            default="stg-hdpashique101:6667",
            type="string",
            title="Bootstrap Servers",
            description="Comma-separated list of Kafka brokers (e.g., host1:port,host2:port).",
        ),
        
        # ==================== Optional Overrides ====================
        "opentsdb_url": Param(
            default="",
            type=["string", "null"],
            title="OpenTSDB URL (Optional)",
            description="URL for CPU monitoring (e.g., http://opentsdb.example.com:4242).",
        ),
        "state_directory": Param(
            default="/data/kafka_demotion_state",
            type=["string", "null"],
            title="State Directory (Optional)",
            description="Path to store decommission state files.",
        ),
        "cpu_threshold": Param(
            default=80.0,
            type=["number", "null"],
            title="CPU Threshold % (Optional)",
            description="Max CPU for leader selection (default: 80).",
        ),
        "disk_threshold": Param(
            default=85.0,
            type=["number", "null"],
            title="Disk Threshold % (Optional)",
            description="Max disk usage for leader selection (default: 85).",
        ),
        "log_level": Param(
            default="INFO",
            type="string",
            title="Runner Log Level",
            enum=["DEBUG", "INFO", "WARNING", "ERROR"],
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

dry_run_action = PythonOperator(
    task_id="dry_run_action",
    python_callable=_execute_salt_task,
    op_kwargs={"dry_run": True},
    dag=dag,
)

execute_action = PythonOperator(
    task_id="execute_action",
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
validate_input >> generate_yaml >> dry_run_action >> execute_action >> verify_result >> generate_report
