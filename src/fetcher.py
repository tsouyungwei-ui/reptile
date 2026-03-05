import time
import random
import requests
import logging
from . import config
import urllib3

# 關閉不安全請求警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定基本日誌記錄
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class fetcher:
    """提供共用的 HTTP 發送功能，內建防封鎖(延遲)與失敗重試機制"""
    
    # 建立一個全局的 Session 來維持 Cookies
    _session = requests.Session()
    
    @staticmethod
    def get_headers():
        """產生隨機 User-Agent 的 Headers，並加入 MOPS 必須的參數"""
        return {
            "User-Agent": random.choice(config.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-TW,zh;q=0.8,en-US;q=0.5,en;q=0.3",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://mops.twse.com.tw",
            "Referer": "https://mops.twse.com.tw/mops/web/t163sb04",
            "Host": "mops.twse.com.tw"
        }
    
    @staticmethod
    def sleep_randomly():
        """隨機睡眠 3~7 秒以避免存取過快"""
        delay = random.uniform(config.INTER_COMPANY_MIN_DELAY, config.INTER_COMPANY_MAX_DELAY)
        time.sleep(delay)

    @classmethod
    def robust_post(cls, url, data=None, max_retries=config.RETRY_COUNT):
        """強化版 POST 請求，先取得首頁 Cookie 再送出查詢"""
        for count in range(max_retries):
            cls.sleep_randomly()
            headers = cls.get_headers()
            
            # 建立短暫的 Session，避免舊的 Cookie 殘留導致 THE PAGE CANNOT BE ACCESSED
            temp_session = requests.Session()
            
            try:
                # 1. 先用 GET 訪問 MOPS 首頁或損益表查詢頁獲取合法的 Session ID (jsessionid)
                temp_session.get("https://mops.twse.com.tw/mops/web/t163sb04", headers=headers, timeout=10, verify=False)
                
                # 2. 再帶著這個合法的 Cookie 發送 POST 取得資料
                response = temp_session.post(url, headers=headers, data=data, timeout=15, verify=False)
                
                # 檢查 HTTP 狀態碼
                response.raise_for_status()
                
                # 簡單檢查回傳是否為系統過載訊息（這在 MOPS 很常見）
                if "查詢過於頻繁" in response.text or "Error" in response.text:
                    logger.warning(f"可能遇到流量限制 ({count+1}/{max_retries})，稍後重試...")
                    time.sleep(10) # 遇到限制時多等 10 秒
                    continue
                    
                # 檢查是否又被擋了
                if "THE PAGE CANNOT BE ACCESSED" in response.text or "頁面無法執行" in response.text:
                    logger.warning(f"網頁回傳 THE PAGE CANNOT BE ACCESSED ({count+1}/{max_retries})，強制冷卻休眠 60 秒...")
                    time.sleep(60)
                    continue
                    
                return response

            except requests.RequestException as e:
                logger.error(f"連線失敗 {url}: {e}")
                time.sleep(10) # 失敗時預設等待

        logger.error(f"POST {url} 達到最大重試次數，請求完全失敗。建議更換 IP 或稍後再試。")
        return None

    @classmethod
    def robust_get(cls, url, params=None, max_retries=config.RETRY_COUNT):
        """強化版 GET 請求，遭遇失敗會自動等待與重試"""
        for count in range(max_retries):
            cls.sleep_randomly()
            headers = cls.get_headers()
            try:
                response = cls._session.get(url, headers=headers, params=params, timeout=15, verify=False)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.error(f"連線失敗 {url}: {e}")
                time.sleep(5)
                
        logger.error(f"GET {url} 達到最大重試次數，請求失敗。")
        return None
