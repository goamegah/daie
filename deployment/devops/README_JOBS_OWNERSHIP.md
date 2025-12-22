# Transfer Jobs Ownership to Service Principal

## Pourquoi transférer l'ownership?

**Best Practice**: Les jobs Databricks doivent être owned par un Service Principal plutôt qu'un utilisateur pour:
- ✅ **Automatisation**: Pas de dépendance aux comptes utilisateurs
- ✅ **Sécurité**: Permissions contrôlées et auditables
- ✅ **Stabilité**: Les jobs continuent de fonctionner même si l'utilisateur quitte l'équipe
- ✅ **Cohérence**: Tous les jobs utilisent la même identité

## Utilisation

### Via Pipeline (Recommandé)

1. Aller dans **Actions** > **CD - Déploiement Databricks**
2. Cliquer **Run workflow**
3. Cocher **🔐 Transférer ownership des jobs au SP**
4. Lancer le workflow

Le script transfère automatiquement l'ownership de tous les jobs du développeur au Service Principal.

### Via Script Local

```bash
# Transférer tous les jobs de l'environnement
python deployment/devops/transfer_jobs_ownership.py dev

# Transférer uniquement les jobs d'un développeur spécifique
python deployment/devops/transfer_jobs_ownership.py dev john
```

**Variables d'environnement requises:**
- `DATABRICKS_HOST`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`

## Que fait le script?

1. **Liste les jobs** (filtrés par tag Developer si spécifié)
2. **Transfère l'ownership** au Service Principal
3. **Donne CAN_MANAGE** au groupe admins (backup)

## Après le transfert

Une fois l'ownership transféré, vous devez mettre à jour la configuration du job:

1. Aller dans le job Databricks
2. **Edit** > **Advanced** > **Run as**
3. Sélectionner **Service Principal**
4. Choisir: `SP GitHub Actions - dev` (ou votre SP)
5. **Save**

Le job s'exécutera maintenant avec l'identité du Service Principal qui a:
- ✅ Permissions Unity Catalog (catalogs, volumes, external locations)
- ✅ Permissions Storage (Storage Blob Data Contributor)
- ✅ Permissions cluster (create, manage)

## Filtrage par Developer

Le script filtre automatiquement les jobs par le tag `Developer` si spécifié.

**Pour que ça fonctionne**, vos jobs doivent avoir le tag:
```python
# Lors de la création du job
tags = {
    "Developer": "john",
    "Environment": "dev"
}
```

## Permissions requises

Le script nécessite que l'utilisateur qui l'exécute ait:
- Permission **CAN_MANAGE** sur les jobs à transférer
- Ou être **admin** du workspace

## Troubleshooting

**Erreur: "You do not have the required permissions"**
- Vous n'êtes pas owner/admin du job
- Demandez à l'owner actuel de transférer l'ownership

**Erreur: "Service Principal not found"**
- Le SP n'existe pas dans le workspace
- Vérifiez que Terraform a bien créé le SP

**Jobs ne s'exécutent pas après transfert**
- Vérifiez que "Run as" est configuré sur le SP
- Vérifiez les permissions storage du SP (Storage Blob Data Contributor)
