import json

import requests
from airflow.models import Variable


class PublicDataApiClient:
    def __init__(self):
        self.base_url = "http://apis.data.go.kr"
        self.default_params = {
            "serviceKey": Variable.get("publicdata_api_key"),
        }

    def make_request(self, method, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        all_params = {**self.default_params, **(params or {})}

        try:
            response = requests.request(method, url, params=all_params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API 요청 실패: {e}")
        except json.JSONDecodeError:
            raise RuntimeError("응답 데이터가 JSON 형식이 아님")
