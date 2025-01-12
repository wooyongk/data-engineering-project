import json

import requests
from airflow.models import Variable


class KISApiClient:
    def __init__(self):
        self.base_url = "https://openapi.koreainvestment.com:9443"

    def _get_headers(self, tr_id=None):
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {Variable.get('kis_access_token')}",
            "appkey": Variable.get("kis_app_access_key"),
            "appsecret": Variable.get("kis_app_secret_key"),
        }
        if tr_id:
            headers["tr_id"] = tr_id
        return headers

    def make_request(self, method, endpoint, params=None, data=None, tr_id=None):
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers(tr_id=tr_id)

        try:
            response = requests.request(
                method, url, headers=headers, params=params, data=data
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API 요청 실패: {e}")
        except json.JSONDecodeError:
            raise RuntimeError("응답 데이터가 JSON 형식이 아님")
