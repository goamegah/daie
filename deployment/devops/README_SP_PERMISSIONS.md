# Service Principal Permissions Management

## Problème

Pour utiliser un Service Principal comme "Run as" dans les jobs Databricks, vous devez avoir la permission **CAN_USE** sur ce SP.

**Erreur typique:**
```
You cannot set the job's identity to <sp-id> because you do not have the required permissions.
Users with the Service Principal Manager role do not inherit the Service Principal User role.
```

## Solution

Le script `grant_sp_permissions.py` vous permet de vous accorder la permission CAN_USE sur le Service Principal.

## Utilisation

### Via Pipeline (Recommandé pour la première fois)

1. Aller dans **Actions** > **CD - Déploiement Databricks**
2. Cliquer **Run workflow**
3. Cocher **🔑 S'accorder permission CAN_USE sur le SP**
4. Lancer le workflow

**Note**: À faire **une seule fois par développeur**. Une fois la permission accordée, elle reste active.

### Via Script Local

```bash
python deployment/devops/grant_sp_permissions.py dev
```

**Variables d'environnement requises:**
- `DATABRICKS_HOST`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`

## Workflow Complet (Première Configuration)

Pour configurer complètement l'utilisation du SP dans les jobs:

### Étape 1: S'accorder la permission CAN_USE
```bash
python deployment/devops/grant_sp_permissions.py dev
```
✅ Vous pouvez maintenant utiliser le SP comme "Run as"

### Étape 2: Transférer l'ownership des jobs
```bash
python deployment/devops/transfer_jobs_ownership.py dev
```
✅ Le SP devient owner de tous les jobs

### Étape 3: Configurer "Run as" dans les jobs
1. Aller dans le job Databricks
2. **Edit** > **Advanced** > **Run as**
3. Sélectionner **Service Principal**
4. Choisir: `SP GitHub Actions - dev`
5. **Save**

✅ Le job s'exécute maintenant avec l'identité du SP

## Via Pipeline (Tout en une fois)

Vous pouvez aussi tout faire via le pipeline:

1. Actions > CD - Déploiement Databricks
2. Cocher:
   - ✅ **🔑 S'accorder permission CAN_USE sur le SP**
   - ✅ **🔐 Transférer ownership des jobs au SP**
3. Lancer

Ensuite, configurez manuellement "Run as" dans chaque job (étape 3 ci-dessus).

## Fréquence d'utilisation

- **grant_sp_permissions.py**: Une seule fois par développeur
- **transfer_jobs_ownership.py**: À chaque fois que vous créez de nouveaux jobs

## Permissions accordées

Le script accorde **CAN_USE** qui permet:
- ✅ Utiliser le SP comme "Run as" dans les jobs
- ✅ Exécuter des jobs avec l'identité du SP
- ❌ Ne permet PAS de modifier le SP lui-même

## Sécurité

Le script utilise les credentials du SP pour s'accorder des permissions à lui-même. C'est sécurisé car:
- Le SP peut gérer ses propres permissions
- Seuls les users autorisés ont accès aux credentials du SP (via GitHub Secrets)
- La permission CAN_USE est limitée (pas de modification du SP)

## Troubleshooting

**Erreur: "Service Principal not found"**
- Vérifiez que le SP existe dans Databricks
- Vérifiez que Terraform a bien créé le SP

**Erreur: "Permission denied"**
- Le SP n'a pas le droit de modifier ses propres permissions
- Contactez un admin workspace

**La permission ne fonctionne pas**
- Déconnectez-vous et reconnectez-vous à Databricks UI
- Attendez quelques secondes (propagation des permissions)
