IMAGE_NAME=$1
TAG=$2

echo "[INFO] Building Docker image..."
docker build . -t "$IMAGE_NAME:$TAG" -f Dockerfile

echo "[INFO] Authenticating with GCP..."
gcloud auth configure-docker us-west1-docker.pkg.dev

echo "[INFO] Pushing Docker image..."
docker push "$IMAGE_NAME:$TAG"

echo '[INFO] Done. Image built and pushed.'
