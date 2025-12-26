# Scripts de Déploiement DevOps

## 🖥️ manage_cluster.py

Crée ou supprime des clusters Databricks pour les développeurs.

### Usage
```bash
python manage_cluster.py <create|delete> <environment> <developer_name>
```

### Exemples
```bash
# Créer un cluster pour john
python manage_cluster.py create dev john

# Supprimer le cluster de john
python manage_cluster.py delete dev john
```

### Configuration du cluster créé
- **Nom** : `daie-{env}-{developer}`
- **Mode** : Unity Catalog (USER_ISOLATION)
- **Type** : Single node (Standard_D4ds_v5)
- **Auto-termination** : 20 minutes
- **Tags** : Developer, Environment, ManagedBy=Pipeline
- **Package** : Installé automatiquement depuis le volume
- **Init script** : Configuré automatiquement

---

## 📦 deploy_artifacts.py

Déploie tous les types d'artefacts vers Unity Catalog Volumes.

### Usage
```bash
python deploy_artifacts.py <type> <env> [developer_name]
```

### Types supportés
- `metadata` - Métadonnées des sources (par développeur)
- `config` - Fichiers de configuration (par développeur)
- `schema` - Schémas de données (par développeur)
- `init_scripts` - Scripts d'initialisation des clusters (par développeur)

### Comportement
Tous les artefacts sont déployés dans `/Volumes/.../artifacts/{type}/{developer_name}/`

### Exemples
```bash
# Déployer metadata pour le développeur john
python deploy_artifacts.py metadata dev john
# → /Volumes/daie_chn_dev_bronze/artifacts/metadata/john/

# Déployer init scripts pour john
python deploy_artifacts.py init_scripts dev john
# → /Volumes/daie_chn_dev_bronze/artifacts/init_scripts/john/

# Déployer config par défaut
python deploy_artifacts.py config dev
# → /Volumes/daie_chn_dev_bronze/artifacts/config/dev/
```

### Variables d'environnement requises
- `DATABRICKS_HOST`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`

---

## 🎯 install_package_on_clusters.py

Installe le package daie sur les clusters d'un développeur spécifique.

### Usage
```bash
python install_package_on_clusters.py <environment> [developer_name]
```

### Exemples
```bash
# Installer sur les clusters du développeur "john"
python install_package_on_clusters.py dev john

# Installer sur les clusters par défaut
python install_package_on_clusters.py dev
```

### Filtrage
Les clusters sont filtrés par le tag `Developer`. Assurez-vous que vos clusters ont ce tag :
```hcl
custom_tags = {
  "Developer" = "john"
}
```

---

## 🚀 Via GitHub Actions

1. Aller dans **Actions** > **CD - Déploiement Databricks**
2. Cliquer sur **Run workflow**
3. Remplir :
   - **Environnement** : dev/test/prod
   - **Nom développeur** : votre nom
   - **🖥️ Gérer le cluster** : 
     - `none` - Ne rien faire
     - `create` - Créer votre cluster
     - `delete` - Supprimer votre cluster
   - Cocher les artefacts à déployer

Le package sera automatiquement installé sur vos clusters.

### Workflow typique

```bash
# 1. Premier déploiement - Créer le cluster
Gérer le cluster: create
→ Crée daie-dev-john avec le package installé

# 2. Mise à jour du code - Juste déployer
Gérer le cluster: none
→ Met à jour le package, réinstalle sur clusters existants

# 3. Fin de journée - Supprimer le cluster
Gérer le cluster: delete
→ Supprime daie-dev-john pour économiser
```
