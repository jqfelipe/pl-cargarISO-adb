# Pasos para habilitar conexión a base de datos Databricks
1. Hay que tener configurado un Access Connector for Azure Databricks
- Habilitar Identity (On)

2. Conéctate como administrador Entra ID al Azure SQL y ejecuta:

```sql
CREATE USER [labdatabricks-bncr-ac] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader
ADD MEMBER [labdatabricks-bncr-ac];
ALTER ROLE db_datawriter
ADD MEMBER [labdatabricks-bncr-ac];
```
Lo ideal de la conexión con Databricks es usar Identidad Administrada
- Utilizando Access Connector for Azure Databricks
- En Databricks Catalog -> Connect -> Credentials -> Create credential (Managed Identity)
- Luego en en IAM de los elementos a consumir se agrega el rol con el Service Principal del access conector

```python
# Azure SQL y Service Bus - autenticación via Service Credential (Managed Identity)
_service_credential = dbutils.credentials.getServiceCredentialsProvider('bncr-dati-conector')
```

También en Databricks se pueden crear secrets utilizando un terminal pero como última opción

```bash
databricks secrets create-scope dblotes

databricks secrets put-secret --json '{ "scope": "dblotes", "key": "credenciales", "string_value": "{\"usuario\": \"NombreUsuario\", \"password\": \"ClaveUsuario\"}" }'
```

Para obtener esas credenciales se usa lo siguiente
```python
import json
_creds = json.loads(dbutils.secrets.get(scope="bdlotes", key="credenciales"))
SQL_USER = _creds["usuario"]
SQL_PASS = _creds["password"]
```

