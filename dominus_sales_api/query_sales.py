import requests
import time
from dominus_sales_api.config import client_id, client_secret, scope

# Define the URL and the endpoints for the API
base_url = "https://dominus.iapropiada.com/"
auth_endpoint = "oauth/v2/token"
sales_endpoint = "integrations/administrative/dominus/sales"


class TokenManager:
    def __init__(self, client_id, client_secret, scope):
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.token = None
        self.token_expiration = 0

    def get_token(self):
        # Verificar si el token es nulo o ha expirado
        if self.token is None or time.time() >= self.token_expiration:
            self.token = self.request_new_token()
        return self.token

    def request_new_token(self):
        # Payload de autenticación
        auth_payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": self.scope
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        # Solicitar nuevo token
        auth_response = requests.post(f"{base_url}{auth_endpoint}", data=auth_payload, headers=headers)
        if auth_response.status_code == 200:
            auth_data = auth_response.json()
            self.token_expiration = time.time() + auth_data.get("expires_in", 3600) - 60  # Restar 60s de seguridad
            return auth_data.get("access_token")
        else:
            raise Exception(f"Failed to obtain token: {auth_response.json()}")

def query_sales_by_date(token, start_date, branch_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "start_date": start_date,
        "branch_id": branch_id
    }

    # Usar el método GET para consultar ventas
    response = requests.get(f"{base_url}{sales_endpoint}", json=payload, headers=headers)
    return response.json()


def query_sales_eds(branch_id, start_date):
    try:
        # Obtener token utilizando el TokenManager
        token_manager = TokenManager(client_id, client_secret, scope)
        token = token_manager.get_token()

        # Consultar ventas con el token
        sales_data = query_sales_by_date(token, start_date, branch_id)
        return sales_data
    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    branch_id = 2157  # Ejemplo de sucursal
    start_date = "2024-07-01"  # Ejemplo de fecha
    query_sales_eds(branch_id, start_date)


