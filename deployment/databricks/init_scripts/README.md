# Init Scripts Databricks

Ce dossier contient les scripts d'initialisation pour les clusters Databricks.

## 📦 install_daie_package.sh

Script qui installe automatiquement le package `daie` au démarrage du cluster.

### Utilisation

#### Option 1 : Via Terraform (Recommandé)

Le script est automatiquement configuré dans `deployment/terraform/dev/cluster.tf` :

```hcl
resource "databricks_cluster" "daie_dev" {
  # ... autres configurations ...
  
  custom_tags = {
    "Developer" = var.developer_name  # Important pour l'init script
  }
  
  init_scripts {
    volumes {
      destination = "/Volumes/daie_chn_dev_bronze/artifacts/init_scripts/${var.developer_name}/install_daie_package.sh"
    }
  }
}
```

**Déployer avec Terraform :**
```bash
cd deployment/terraform/dev
terraform apply -var="developer_name=john"
```

#### Option 2 : Via l'interface Databricks

1. Allez dans **Compute** > Sélectionnez votre cluster
2. Cliquez sur **Edit**
3. Allez dans **Advanced options** > **Init Scripts**
4. Ajoutez le chemin : `/Volumes/daie_chn_dev_bronze/artifacts/init_scripts/{votre_nom}/install_daie_package.sh`
5. Dans **Tags**, ajoutez : `Developer = {votre_nom}`
6. Cliquez sur **Confirm**

#### Option 3 : Via l'API Databricks

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.clusters.edit(
    cluster_id="your-cluster-id",
    init_scripts=[{
        "volumes": {
            "destination": "/Volumes/daie_chn_dev_bronze/artifacts/init_scripts/install_daie_package.sh"
        }
    }]
)
```

### Déploiement

Les init scripts sont déployés **par développeur** via le pipeline CD :

```bash
# Via GitHub Actions
# Allez dans Actions > CD - Déploiement Databricks > Run workflow
# Remplissez "Nom développeur" : john
# Cochez "🚀 Déployer init scripts"
```

Ou manuellement via CLI :

```bash
# Déployer les init scripts pour john
python deployment/devops/deploy_artifacts.py init_scripts dev john
# → /Volumes/daie_chn_dev_bronze/artifacts/init_scripts/john/

# Déployer pour le développeur par défaut
python deployment/devops/deploy_artifacts.py init_scripts dev
# → /Volumes/daie_chn_dev_bronze/artifacts/init_scripts/dev/
```

**Avantages** : Chaque développeur peut avoir sa propre version de l'init script si nécessaire.

### Vérification

Pour vérifier que le script fonctionne :

1. Démarrez le cluster
2. Ouvrez un notebook
3. Exécutez :

```python
import daie
print(daie.__version__)
```

### Logs

Les logs du script d'initialisation sont disponibles dans :
- **Databricks UI** : Compute > Cluster > Event Log > Init Scripts
- **Fichier** : `/databricks/init_scripts/logs/`

### Troubleshooting

Si le package n'est pas installé :

1. Vérifiez que le wheel existe dans le volume :
   ```python
   dbutils.fs.ls("/Volumes/daie_chn_dev_bronze/artifacts/packages/dev/")
   ```

2. Vérifiez les logs d'init script dans l'Event Log du cluster

3. Vérifiez que le script a les bonnes permissions :
   ```bash
   # Le script doit être exécutable
   ls -la /Volumes/daie_chn_dev_bronze/artifacts/init_scripts/
   ```

### Modification du script

Pour modifier le script :

1. Éditez `deployment/databricks/init_scripts/install_daie_package.sh`
2. Committez et poussez les changements
3. Exécutez le pipeline CD
4. Redémarrez le cluster pour appliquer les changements
