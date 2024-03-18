import urllib.parse

import requests
from airflow.exceptions import AirflowException


class ZettleMixin:
    def get_zettle_access_token(self, grant_type, client_id, api_key):
        try:
            url = "https://oauth.izettle.com/token"
            data = {
                "grant_type": grant_type,
                "client_id": client_id,
                "assertion": api_key,
            }
            headers = {
                "content-type": "application/x-www-form-urlencoded",
            }
            response = requests.post(url, data=urllib.parse.urlencode(data), headers=headers)

            if response.status_code == 200 and response.json():
                data = response.json()
                return data.get("access_token")
        except Exception as e:
            self.log.error("Error during database operation: %s", e)
            raise AirflowException(f"Error getting Zettle Access Token: {e}")
