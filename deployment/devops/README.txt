DÉPLOIEMENT D'ARTIFACTS
========================

Usage: python deploy_artifacts.py <type> <env> [developer_name]

Types supportés: metadata, config, schema
Environnements: dev, test, prod

Exemples:
  python deploy_artifacts.py metadata dev
  python deploy_artifacts.py metadata dev john
  python deploy_artifacts.py config prod

Destination:
  /Volumes/daie_chn_{env}_bronze/artifacts/{type}/{developer_name ou 'dev'}/

Via GitHub Actions:
  1. Aller dans Actions > "CD - Déploiement Databricks"
  2. Run workflow
  3. Remplir:
     - Environnement: dev/test/prod
     - Nom développeur: votre nom
     - Cocher les artifacts à déployer:
       ☑ Déployer metadata
       ☑ Déployer config
       ☑ Déployer schema

Variables d'environnement requises:
  DATABRICKS_HOST
  AZURE_CLIENT_ID
  AZURE_CLIENT_SECRET
  AZURE_TENANT_ID
