import urllib.request
import json
from datetime import datetime, timezone

def get_token(resource, service_credential):
    """Obtiene token OAuth fresco. Encapsula dbutils para serialización del sink."""
    return service_credential.get_token(resource)

# =============================================================================
# Helper: Notificación a Azure Service Bus
# Envía mensaje al topic tras escritura exitosa en SQL vía REST API.
# Autenticación via Bearer token (OAuth) del Service Credential.
# =============================================================================
def send_service_bus_notification(sb_topic_endpoint, file_name, correlation_id, sb_access_token):
    """Envía notificación al topic de Azure Service Bus."""
    payload = json.dumps({
        "nombreArchivo": file_name,
        "correlationId": correlation_id,
        "estado": "procesado",
        "fechaProceso": datetime.now(timezone.utc).isoformat(),
    })

    send_url = f"{sb_topic_endpoint}/messages"

    req = urllib.request.Request(
        send_url,
        data=payload.encode("utf-8"),
        headers={
            "Authorization": f"Bearer {sb_access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    urllib.request.urlopen(req)

def move_file(origin, dest, file):
    """Mueve archivo a carpeta de archivos procesados."""
    src = f"{origin}/{file}"
    dst = f"{dest}/{file}"
    dbutils.fs.mv(src, dst)
