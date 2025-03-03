# Autenticação
gcloud auth login

# Configurar o projeto
gcloud config set project <PROJET_ID>

# Deploy da Cloud Function
gcloud functions deploy cf_financial_extract \
  --region=us-east4 \
  --runtime=python312 \
  --source=gs://<source-bucket>/cf_financial_extract_v1.0 \
  --entry-point=main \
  --trigger-http \
  --allow-unauthenticated \
  --memory=256MB

