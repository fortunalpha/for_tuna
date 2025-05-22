import os
import requests as rq
import dill
from datetime import datetime as dt
from dataclasses import dataclass 
from typing import Optional
from dotenv import load_dotenv
from exchange import PROJECT_ROOT_PATH
    
class _Token():
    # TOKEN_LEGTH = 350
    def __init__(self, *, access_token, token_type, expires_in, access_token_token_expired) -> None:
        self._access_token = access_token
        self.token_type = token_type
        self.expires_in = expires_in
        self.expiration_time = dt.strptime(access_token_token_expired, '%Y-%m-%d %H:%M:%S')
        self.token = f'{self.token_type} {self._access_token}'

@dataclass
class Position:
    size: int
    
@dataclass
class Order:
    timestamp: dt
    id: int

@dataclass
class EtfPrice:
    current_price: float
    nav: float
    disparity: float

class KoreaInvestment(): 
    def __init__(self) -> None:
        load_dotenv(override=True)
        self._access_key = os.getenv('KI_ACCESS_KEY')
        self._secret_key = os.getenv('KI_SECRET_KEY')
        self._account_num = os.getenv('KI_ACCOUNT_NUM')
        sep_idx = self._account_num.index('-')
        self._account_num_first_8digit = self._account_num[:sep_idx]
        self._account_num_last_2digit = self._account_num[sep_idx + 1:]

        if self._access_key == None or self._secret_key == None or self._account_num == None:
            raise ValueError('Cannot found KoreaInvestment API keys')
        self._url = 'https://openapi.koreainvestment.com:9443' # not paper trading
        self._ws_url = 'https://openapi.koreainvestment.com:9443' # not paper trading
        self._token_file_path = os.path.join(PROJECT_ROOT_PATH, 'ki_access_token.dill')
        self._token = self._get_auth_token()
        self._ws_key = self._get_ws_access_key()

    def _get_auth_token(self) -> _Token:
        if os.path.exists(os.path.abspath(self._token_file_path)):
            with open(self._token_file_path, 'rb') as f:
                token = dill.load(f)
            if self._check_valid_token(token):
                return token
            
        return self._fetch_auth_token()
    
    def _fetch_auth_token(self) -> _Token:
        headers = {
            "content-type": "application/json"
        }
        body = {
            "grant_type": "client_credentials",
            "appkey": self._access_key,
            "appsecret": self._secret_key
        }
        url = self._url + '/oauth2/tokenP'
        res = rq.post(url, json=body, headers=headers)
        if res.status_code != 200:
            raise ConnectionRefusedError(f'{res.text}')
        token = _Token(**res.json())
        with open(self._token_file_path, 'wb') as f:
            dill.dump(token, f)
        
        return token
    
    # assume that token type is only Bearer
    def _check_valid_token(self, token) -> bool:
        if isinstance(token, _Token) == False:
            return False
        
        if token.token.startswith('Bearer ') == False:
            return False
        
        if token.expiration_time < dt.now():
            return False
        
        return True
    
    def _get_ws_access_key(self) -> str:
        headers = {
            "content-type": "application/json"
        }
        body = {
            "grant_type": "client_credentials",
            "appkey": self._access_key,
            "secretkey": self._secret_key
        }
        url = self._url + '/oauth2/Approval'
        res = rq.post(url, json=body, headers=headers)
        if res.status_code != 200:
            raise ConnectionRefusedError(f'{res.text}')

        return res.json()['approval_key']

    def fetch_account_cash(self) -> float:
        headers = self._make_appkey_header("TTTC8908R")
        url = self._url + '/uapi/domestic-stock/v1/trading/inquire-psbl-order'
        params = self._make_account_num_param(
            PDNO='',
            ORD_UNPR='',
            ORD_DVSN='00',
            CMA_EVLU_AMT_ICLD_YN='N',
            OVRS_ICLD_YN='N'
        )
        res = rq.get(url, headers=headers, params=params)
        self._handle_server_response(res)

        data = res.json()
        if data['rt_cd'] != '0':
            raise ValueError(f'{data["rt_cd"]} {data["msg1"]}')
        
        return data['output']['nrcvb_buy_amt']
    
    def fetch_open_position(self, code) -> Optional[Position]:
        headers = self._make_appkey_header("TTTC8434R")
        url = self._url + '/uapi/domestic-stock/v1/trading/inquire-balance'
        params = self._make_account_num_param(
            AFHR_FLPR_YN='N',
            OFL_YN='',
            INQR_DVSN='02',
            UNPR_DVSN='01',
            FUND_STTL_ICLD_YN='N',
            FNCG_AMT_AUTO_RDPT_YN='N',
            PRCS_DVSN='00',
            CTX_AREA_FK100='',
            CTX_AREA_NK100=''
        )
        res = rq.get(url, headers=headers, params=params)
        self._handle_server_response(res)
        
        data = res.json()
        if data['rt_cd'] != '0':
            raise ValueError(f'{data["rt_cd"]} {data["msg1"]}')
        
        found = None
        for open_position in data['output1']:
            if open_position['pdno'] == code:
                found = Position(int(open_position['hold_qty']))
                
        return found
    
    def submit_order(self, order: dict) -> dict:
        if order['side'] == 'buy':
            tr_id = 'TTTC0802U'
        else: # sell side
            tr_id = 'TTTC0801U'
        if order['type'] == 'limit':
            order_type = '00'
        else: # market order
            order_type = '01'
        headers = self._make_appkey_header(tr_id)
        body = {
            "CANO": self._account_num_first_8digit,
            "ACNT_PRDT_CD": self._account_num_first_8digit,
            "PDNO": order['code'],
            "ORD_DVSN": order_type,
            "ORD_QTY": str(order['size']),
            "ORD_UNPR": str(order['price']),
        }
        url = self._url + '/uapi/domestic-stock/v1/trading/order-cash'
        res = rq.post(url, json=body, headers=headers)
        
        self._handle_server_response(res)
        
        data = res.json()
        if data['rt_cd'] != '0':
            raise ValueError(f'{data["rt_cd"]} {data["msg1"]}')

        timestamp = dt.strptime(data['output']['ORD_TMD'], '%H%M%S')
        now_ts = dt.now().replace(hour=timestamp.hour, minute=timestamp.minute, second=timestamp.second)
        return Order(now_ts, int(data['output']['ORD_NO']))
    
    def fetch_orderbook(self, code) -> dict:
        headers = self._make_appkey_header("FHKST01010200")
        url = self._ws_url + '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn'    
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code
        }
        res = rq.get(url, headers=headers, params=params)
        self._handle_server_response(res)
        
        data = res.json()
        if data['rt_cd'] != '0':
            raise ValueError(f'{data["rt_cd"]} {data["msg1"]}')
        
        for k, v in data['output1'].items():
            data['output1'][k] = int(v)
            
        return data['output1']

    def fetch_etf_price(self, code) -> EtfPrice:
        headers = self._make_appkey_header("FHPST02400000")
        url = self._url + '/uapi/etfetn/v1/quotations/inquire-price'
        params = {
            'fid_input_iscd': code,
            'fid_cond_mrkt_div_code': 'J'
        }
        res = rq.get(url, headers=headers, params=params)
        self._handle_server_response(res)
        
        data = res.json()
        if data['rt_cd'] != '0':
            raise ValueError(f'{data["rt_cd"]} {data["msg1"]}')
        
        return EtfPrice(float(data['output']['stck_prpr']), 
                        float(data['output']['nav']),
                        float(data['output']['dprt']))

    def _handle_server_response(self, res) -> None:
        if res.status_code == 200:
            pass
        elif res.status_code == 500: # internal server error
            self._token = self._get_auth_token()
        else:
            raise ConnectionRefusedError(f'{res.status_code} {res.reason}')
        

    def _make_appkey_header(self, tr_id, **kwargs) -> dict :
        self._token = self._get_auth_token()
        return {
            "content-type": "application/json",
            "authorization": self._token.token,
            "appkey": self._access_key,
            "appsecret": self._secret_key,
            "tr_id": tr_id,
            **kwargs
        }    
        
    def _make_ws_header(self) -> dict :
        return {
            "approval_key": self._ws_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        }
        
    def _make_account_num_param(self, **kwargs) -> dict :
        return {
            "CANO": self._account_num_first_8digit,
            "ACNT_PRDT_CD": self._account_num_last_2digit,
            **kwargs
        }