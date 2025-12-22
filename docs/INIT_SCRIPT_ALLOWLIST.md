# Init Script Allowlist Configuration

## Problème

Databricks requiert que les init scripts soient dans une allowlist pour des raisons de sécurité. Si vous voyez cette erreur:

```
INVALID_PARAMETER_VALUE: Attempting to install the following init scripts that are not in the allowlist
```

C'est que l'init script n'est pas autorisé.

## Solutions

### Option 1: Via le workflow GitHub Actions (Automatique)

Le workflow CD configure automatiquement l'allowlist avant de créer le cluster. Assurez-vous que le Service Principal a les permissions nécessaires.

### Option 2: Via le script Python

```bash
# Depuis le workflow GitHub Actions (avec les secrets configurés)
python deployment/devops/configure_init_script_allowlist.py dev
```

### Option 3: Via le script Bash

```bash
# Exporter les variables d'environnement
export DATABRICKS_HOST="https://adb-xxxxx.azuredatabricks.net"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_TENANT_ID="your-tenant-id"

# Exécuter le script
./deployment/devops/configure_allowlist_manual.sh dev
```

### Option 4: Configuration manuelle via l'UI Databricks

1. Allez dans **Admin Console** (icône engrenage en haut à droite)
2. **Workspace Settings** → **Advanced**
3. Activez **Init script allowlist**
4. Ajoutez le pattern: `/Volumes/daie_chn_dev_bronze/artifacts/init_scripts/*`
5. Cliquez **Save**

## Pattern d'allowlist

Pour autoriser tous les init scripts du projet:

- **Dev**: `/Volumes/daie_chn_dev_bronze/artifacts/init_scripts/*`
- **Test**: `/Volumes/daie_chn_test_bronze/artifacts/init_scripts/*`
- **Prod**: `/Volumes/daie_chn_prod_bronze/artifacts/init_scripts/*`

Le wildcard `*` autorise tous les développeurs.

## Permissions requises

Pour configurer l'allowlist, vous devez avoir:
- **Workspace Admin** permissions dans Databricks
- Ou demander à un admin de le faire pour vous

## Vérification

Après configuration, vous pouvez vérifier dans:
- Admin Console → Workspace Settings → Advanced → Init script allowlist

Vous devriez voir votre pattern dans la liste.
