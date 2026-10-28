# Service Account Key Management

## IMPORTANT: Never store key files in Git

## Recommended: Use Workload Identity Federation (keyless)
For Cloud Run, Dataflow, Composer, and Vertex AI workloads running on GCP,
use Workload Identity — no key files needed.

```bash
# For local development: use Application Default Credentials
gcloud auth application-default login

# For CI/CD (Cloud Build): use built-in SA automatically
# No key needed — Cloud Build runs as its own SA

# For external systems (if needed): use Workload Identity Federation
gcloud iam workload-identity-pools create nordleaf-wi-pool \
  --location=global \
  --display-name="NordLeaf Workload Identity Pool"
```

## Secret Manager for Sensitive Config
```bash
# Store connection strings, API keys in Secret Manager
echo -n "your-secret-value" | gcloud secrets create nordleaf-db-password \
  --data-file=-

# Grant Composer SA access to secrets
gcloud secrets add-iam-policy-binding nordleaf-db-password \
  --member="serviceAccount:nordleaf-composer@nordleaf-project.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```
