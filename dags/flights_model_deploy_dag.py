"""
Airflow DAG: flights_model_deploy
Chapter 10: Model Deployment & Serving with Seldon Core on Kubernetes

Pipeline Architecture:
  Task 1 (build_push_image):
    - Connects to MLflow Tracking Server (http://mlflow:5500) and MinIO (http://minio-ml-workshop:9000).
    - Downloads the trained model artifacts (model.pkl and FlgithsDelayOrdinalEncoder.pkl).
    - Uses Kaniko inside Kubernetes to containerize the serving application (Python 3.11, Transformer, Predictor).
    - Pushes the image to Docker Hub (or configured container registry).

  Task 2 (deploy_model):
    - Uses Kubernetes service account credentials in ml-workshop.
    - Renders SeldonDeploy.yaml and Ingress.yaml using Jinja2 with MLflow run_id metadata.
    - Deploys the multi-stage Seldon Core inference service (Transformer -> Predictor) and NGINX Ingress.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable

try:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
except ImportError:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# =============================================================================
# Deployment Configuration
# Configured in Airflow UI -> Admin -> Variables:
#   DOCKERHUB_USER
#   DOCKERHUB_PASSWORD
# =============================================================================
DOCKERHUB_USER = Variable.get("DOCKERHUB_USER", default_var="YOUR_DOCKERHUB_USERNAME")
DOCKERHUB_PASSWORD = Variable.get("DOCKERHUB_PASSWORD", default_var="YOUR_DOCKERHUB_TOKEN_OR_PASSWORD")

MODEL_NAME = Variable.get("MODEL_NAME", default_var="flights-ontime")
MODEL_VERSION = Variable.get("MODEL_VERSION", default_var="1")
IMAGE_TAG = Variable.get("MODEL_IMAGE_TAG", default_var="1.0.0")

CONTAINER_REGISTRY = "https://index.docker.io/v1/"
CONTAINER_DETAILS = f"{DOCKERHUB_USER}/flight-delay-model:{IMAGE_TAG}"
CLUSTER_DOMAIN_NAME = Variable.get("CLUSTER_DOMAIN_NAME", default_var="192.168.49.2.nip.io")

default_args = {
    'owner': 'ml-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'flights_model_deploy',
    default_args=default_args,
    description='Chapter 10: Build model serving container with Kaniko and deploy Seldon inference graph',
    schedule_interval=None,  # Trigger on-demand when a new model is registered
    catchup=False,
    tags=['mlops', 'chapter10', 'seldon', 'model-serving', 'dockerhub'],
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Build serving container with Kaniko and push to Docker Hub
    # -------------------------------------------------------------------------
    build_push_task = KubernetesPodOperator(
        namespace='ml-workshop',
        image='quay.io/ml-on-k8s/kaniko-container-builder:1.0.0',
        cmds=['/bin/bash', '-c'],
        arguments=[
            'set -e && '
            'echo "=== Downloading serving codebase from repository ===" && '
            'rm -rf /tmp/app /tmp/repo.tar.gz /tmp/Machine-Learning* && '
            'if command -v git >/dev/null 2>&1; then '
            '  git clone --depth 1 https://github.com/JacobItopa/Machine-Learning-on-Kubernetes.git /tmp/app; '
            'else '
            '  python3 -c "import urllib.request, tarfile; '
            'urllib.request.urlretrieve(\'https://github.com/JacobItopa/Machine-Learning-on-Kubernetes/archive/refs/heads/main.tar.gz\', \'/tmp/repo.tar.gz\'); '
            'tarfile.open(\'/tmp/repo.tar.gz\').extractall(\'/tmp\')" && '
            '  mv /tmp/Machine-Learning-on-Kubernetes-main /tmp/app; '
            'fi && '
            'cd /tmp/app/chapter10/model_deploy_pipeline/model_build_push && '
            'echo "=== Running build_push_image.py to download artifacts and invoke Kaniko ===" && '
            'python3 build_push_image.py'
        ],
        env_vars={
            'MODEL_NAME': MODEL_NAME,
            'MODEL_VERSION': MODEL_VERSION,
            'CONTAINER_REGISTRY': CONTAINER_REGISTRY,
            'CONTAINER_REGISTRY_USER': DOCKERHUB_USER,
            'CONTAINER_REGISTRY_PASSWORD': DOCKERHUB_PASSWORD,
            'CONTAINER_DETAILS': CONTAINER_DETAILS,
            'AWS_SECRET_ACCESS_KEY': 'minio123',
            'AWS_ACCESS_KEY_ID': 'minio',
        },
        security_context={'run_as_user': 0},
        labels={'app': 'kaniko-model-builder'},
        name='kaniko-build-flights-model',
        task_id='build_push_image',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # -------------------------------------------------------------------------
    # Task 2: Render SeldonDeploy.yaml & Ingress.yaml and apply to Kubernetes
    # -------------------------------------------------------------------------
    deploy_task = KubernetesPodOperator(
        namespace='ml-workshop',
        image='quay.io/ml-on-k8s/airflow-python-runner:0.0.11',
        cmds=['/bin/bash', '-c'],
        arguments=[
            'set -e && '
            'echo "=== Downloading deployment templates from repository ===" && '
            'rm -rf /tmp/app /tmp/repo.tar.gz /tmp/Machine-Learning* && '
            'if command -v git >/dev/null 2>&1; then '
            '  git clone --depth 1 https://github.com/JacobItopa/Machine-Learning-on-Kubernetes.git /tmp/app; '
            'else '
            '  python3 -c "import urllib.request, tarfile; '
            'urllib.request.urlretrieve(\'https://github.com/JacobItopa/Machine-Learning-on-Kubernetes/archive/refs/heads/main.tar.gz\', \'/tmp/repo.tar.gz\'); '
            'tarfile.open(\'/tmp/repo.tar.gz\').extractall(\'/tmp\')" && '
            '  mv /tmp/Machine-Learning-on-Kubernetes-main /tmp/app; '
            'fi && '
            'cd /tmp/app/chapter10/model_deploy_pipeline/model_deploy && '
            'echo "=== Running deploy_model.py to render and apply SeldonDeployment ===" && '
            'python3 deploy_model.py'
        ],
        env_vars={
            'MODEL_NAME': MODEL_NAME,
            'MODEL_VERSION': MODEL_VERSION,
            'CONTAINER_REGISTRY': CONTAINER_REGISTRY,
            'CONTAINER_DETAILS': CONTAINER_DETAILS,
            'CLUSTER_DOMAIN_NAME': CLUSTER_DOMAIN_NAME,
        },
        security_context={'run_as_user': 0},
        labels={'app': 'seldon-model-deployer'},
        name='seldon-deploy-flights-model',
        task_id='deploy_model',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Execution order: build and push container first, then deploy Seldon service
    build_push_task >> deploy_task
