"""
Airflow DAG for Kafka Partition Skewness Balancing
==================================================
Runs the partition balancing script via Salt-Stack.
Now includes Salt credentials from Airflow Connections.

Tasks:
1. validate_input - Validates user inputs
2. generate_yaml - Converts config to YAML string for the script
3. pre_checks_and_dry_run_balance - Executes the script in --dry-run mode
4. execute_balance - Performs the actual execution with --execute --monitor --verify
5. verify_result - Verifies the final execution result
6. generate_report - Creates final report

Author: DevOps Team
Version: 1.5.0 (Fixed InvalidSchema error for connections)
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
from typing import Dict, Any, Optional, Tuple

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Constants
# ============================================================================

# --- MODIFIED FOR PARTITION SKEWNESS ---
DAC_TASK_TYPE = "partition_skewness"
# --- END MODIFICATION ---

# --- NEW: Airflow Connection ID ---
SALT_CONN_ID = "salt_api_default"
# --- END NEW ---

# Set a long timeout, as balancing can take hours
REQUEST_TIMEOUT = 7200  # 2 hours

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
    Task 1: Validate input and build the nested YAML structure.
    """
    logger.info("=" * 80)
    logger.info("TASK 1: Validate Input Configuration (Partition Skewness)")
    logger.info("=" * 80)

    params = context.get("params", {})
    logger.info(f"Received configuration from params: {json.dumps(params, indent=2)}")

    errors = []
    
    # 1. Validate Required Fields
    if not params.get("bootstrap_servers"):
        errors.append("Bootstrap Servers are required.")
    if not params.get("opentsdb_url"):
        errors.append("OpenTSDB URL is required.")

    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(error_msg)
        raise AirflowException(error_msg)

    logger.info("✓ All syntax validations passed")

    # Build the nested YAML config
    yaml_config = {
        "kafka": {
            "bootstrap_servers": params["bootstrap_servers"]
        },
        "opentsdb": {
            "url": params["opentsdb_url"],
            "metrics": {
                "cpu": params.get("opentsdb_metric_cpu", "cpu.field.usage_idle"),
                "disk": params.get("opentsdb_metric_disk", "disk.field.used_percent")
            }
        },
        "thresholds": {
            "skew_percent": params.get("skew_percent", 15.0),
            "replica_skew_percent": params.get("replica_skew_percent", 20.0),
            "disk_max_percent": params.get("disk_max_percent", 85.0),
            "disk_max_diff_percent": params.get("disk_max_diff_percent", 20.0)
        },
        "reassignment": {
            # Convert throttle from MB/s to B/s
            "throttle_bytes_per_sec": int(params.get("throttle_mb_per_sec", 100) * 1024 * 1024),
            "batch_size": params.get("batch_size", 50),
            "batch_wait_seconds": params.get("batch_wait_seconds", 60)
        },
        "advanced": {
            "leader_election_max_retries": params.get("leader_election_max_retries", 2),
            "leader_election_wait_seconds": params.get("leader_election_wait_seconds", 30),
            "tolerance_percent": params.get("tolerance_percent", 15.0)
        }
    }

    # Runner kwargs are simple, just log_level
    runner_kwargs = {
        "log_level": params.get("log_level", "INFO")
    }

    validated_config = {
        "yaml_config": yaml_config,
        "runner_kwargs": runner_kwargs
    }
    
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
    Fetches credentials from Airflow Connection 'salt_api_default'.
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
    logger.info(f"Dry Run Mode: {dry_run}")
    logger.info(f"Runner Kwargs: {runner_kwargs}")

    # Build Salt API payload
    headers = {"Content-Type": "application/json"}

    payload_kwargs = {
        "task_type": DAC_TASK_TYPE,
        "yaml_content": yaml_content,
        "dry_run": dry_run
    }
    
    # Add runner_kwargs (e.g., log_level)
    payload_kwargs.update(runner_kwargs)
    
    # --- MODIFIED: Use new connection variables ---
    payload = {
        "username": salt_api_username,
        "password": salt_api_password,
        "eauth": salt_eauth,
        "client": "runner",
        "fun": "kafka_runner.run_dac_task",
        "kwarg": payload_kwargs
    }
    # --- END MODIFIED ---

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
    salt_result = ti.xcom_pull(task_ids="execute_balance", key="salt_execute_result") # Changed task_id

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
            raise AirflowException(f"Partition balancing failed: {verification_result['message']}")
        
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
    # --- MODIFIED: Use new task ID for xcom_pull ---
    dry_run_salt_result = ti.xcom_pull(task_ids="pre_checks_and_dry_run_balance", key="salt_dry_run_result")
    # --- END MODIFICATION ---
    verification_result = ti.xcom_pull(task_ids="verify_result", key="verification_result")

    if not validated_config:
        logger.warning("Could not retrieve validated config for report.")
        return

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
    print("║" + " KAFKA PARTITION SKEWNESS REPORT ".center(78) + "║")
    print("╚" + "=" * 78 + "╝")
    print("")
    print("  Configuration:")
    print(f"    - bootstrap_servers: {yaml_config.get('kafka', {}).get('bootstrap_servers', 'N/A')}")
    print(f"    - opentsdb_url: {yaml_config.get('opentsdb', {}).get('url', 'N/A')}")
    print(f"    - skew_percent: {yaml_config.get('thresholds', {}).get('skew_percent', 'N/A')}%")
    print(f"    - throttle: {yaml_config.get('reassignment', {}).get('throttle_bytes_per_sec', 0) / 1024 / 1024:.0f} MB/s")

    print("─" * 80)
    print("")
    print(f"  Final Status:  {status}")
    print(f"  Final Message: {message}")
    print("")

    print("  Dry-Run Output (Summary):")
    print("  " + "-" * 76)
    _log_multiline(dry_run_output, prefix="  ")
    print("")

    if status == "✓ SUCCESS":
        print("  Final Execution Output (Summary):")
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
    "kafka_partition_skewness_via_salt",
    default_args=default_args,
    description="Run Kafka partition balancing for skewness via Salt-Stack",
    schedule=None,
    catchup=False,
    tags=["kafka", "salt", "dac", "partition-skewness", "balancing"],

    params={
        # ==================== Required Config ====================
        # Salt params are now fetched from connection 'salt_api_default'
        "bootstrap_servers": Param(
            default="stg-hdpashique101:6667",
            type="string",
            title="Bootstrap Servers",
            description="Comma-separated list of Kafka brokers (e.g., host1:port,host2:port).",
        ),
        "opentsdb_url": Param(
            default="http://opentsdb-read-no-dp-limit.nixy.stg-drove.phonepe.nb6",
            type="string",
            title="OpenTSDB URL",
            description="URL for CPU/Disk monitoring (e.g., http://opentsdb.example.com:4242).",
        ),
        
        # ==================== Reassignment Config ====================
        "throttle_mb_per_sec": Param(
            default=100,
            type="integer",
            title="Throttle (MB/s)",
            description="Reassignment throttle in Megabytes per second.",
            minimum=1,
        ),
        "batch_size": Param(
            default=50,
            type="integer",
            title="Batch Size",
            description="Number of partitions to move in a single batch.",
            minimum=1,
        ),
        "batch_wait_seconds": Param(
            default=60,
            type="integer",
            title="Batch Wait (seconds)",
            description="Time to wait between reassignment batches.",
            minimum=1,
        ),

        # ==================== Thresholds ====================
        "skew_percent": Param(
            default=15.0,
            type="number",
            title="Skew Threshold %",
            description="Leader skew percentage to trigger balancing.",
        ),
        "replica_skew_percent": Param(
            default=20.0,
            type="number",
            title="Replica Skew Threshold %",
            description="Replica skew percentage to trigger balancing.",
        ),
        "disk_max_percent": Param(
            default=85.0,
            type="number",
            title="Max Disk Threshold %",
            description="Brokers above this usage will be avoided.",
        ),
        "disk_max_diff_percent": Param(
            default=20.0,
            type="number",
            title="Max Disk Difference %",
            description="Max allowed disk usage difference between brokers.",
        ),

        # ==================== Advanced ====================
        "leader_election_max_retries": Param(
            default=2,
            type="integer",
            title="Leader Election Retries",
        ),
        "leader_election_wait_seconds": Param(
            default=30,
            type="integer",
            title="Leader Election Wait (sec)",
        ),
        "tolerance_percent": Param(
            default=15.0,
            type="number",
            title="Tolerance %",
        ),

        # ==================== Optional Overrides ====================
        "opentsdb_metric_cpu": Param(
            default="cpu.field.usage_idle",
            type="string",
            title="OpenTSDB CPU Metric (Optional)",
        ),
        "opentsdb_metric_disk": Param(
            default="disk.field.used_percent",
            type="string",
            title="OpenTSDB Disk Metric (Optional)",
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

# --- MODIFIED: Renamed task variable and task_id ---
pre_checks_and_dry_run_balance = PythonOperator(
    task_id="pre_checks_and_dry_run_balance",
    python_callable=_execute_salt_task,
    op_kwargs={"dry_run": True},
    dag=dag,
)
# --- END MODIFICATION ---

execute_balance = PythonOperator(
    task_id="execute_balance",
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
# --- MODIFIED: Use new task name in chain ---
validate_input >> generate_yaml >> pre_checks_and_dry_run_balance >> execute_balance >> verify_result >> generate_report
# --- END MODIFICATION ---
