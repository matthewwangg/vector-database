PROJECT_ID=$1
CLUSTER=$2
REGION=$3

echo "[INFO] Setting GCP project..."
gcloud config set project "$PROJECT_ID"

echo "[INFO] Getting cluster credentials..."
gcloud container clusters get-credentials "$CLUSTER" --region "$REGION" --project "$PROJECT_ID"

echo "Deploying to GKE cluster..."
kubectl apply -f infra/base

echo "Deployment complete. Checking rollout status..."
kubectl rollout status statefulset/vector-database
