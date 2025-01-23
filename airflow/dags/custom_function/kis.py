import json
import time

import requests
from airflow.models import Variable


class KISApiClient:
    def __init__(self):
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.last_request_time = 0
        self.request_interval = 1 / 15  # 초당 15건 제한 / 초당 20건 제한(실제)
        self.headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {Variable.get('kis_access_token')}",
            "appkey": Variable.get("kis_app_access_key"),
            "appsecret": Variable.get("kis_app_secret_key"),
        }

    def _get_headers(self, tr_id=None):
        if tr_id:
            self.headers["tr_id"] = tr_id
        else:
            self.headers.pop("tr_id", None)

        return self.headers

    def make_request(self, method, endpoint, params=None, data=None, tr_id=None):
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers(tr_id=tr_id)

        current_time = time.time()
        elapsed_time = current_time - self.last_request_time
        if elapsed_time < self.request_interval:
            time.sleep(self.request_interval - elapsed_time)

        try:
            response = requests.request(
                method, url, headers=headers, params=params, data=data
            )
            response.raise_for_status()
            self.last_request_time = time.time()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API 요청 실패: {e}")
        except json.JSONDecodeError:
            raise RuntimeError("응답 데이터가 JSON 형식이 아님")
