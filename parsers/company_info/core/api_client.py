"""
API Client for ba.prg.kz
"""

import time
import random
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import (
    API_BASE_URL,
    API_ENDPOINTS,
    API_HEADERS,
    API_TIMEOUT,
    RATE_LIMIT,
    RETRY_POLICY
)
from .logger import logger


class APIError(Exception):
    """API request error."""
    pass


class CompanyNotFoundError(APIError):
    """Company not found in API."""
    pass


class APIClient:
    """
    HTTP client for ba.prg.kz API with rate limiting and retry logic.
    """
    
    def __init__(self):
        self.base_url = API_BASE_URL
        self.session = self._create_session()
        self.last_request_time = 0
    
    def _create_session(self) -> requests.Session:
        """Create session with retry strategy."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=0,  # We handle retries manually
            status_forcelist=[]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _rate_limit(self):
        """Apply rate limiting delay."""
        delay = random.uniform(
            RATE_LIMIT['min_delay_seconds'],
            RATE_LIMIT['max_delay_seconds']
        )
        
        elapsed = time.time() - self.last_request_time
        if elapsed < delay:
            sleep_time = delay - elapsed
            logger.debug(f"Rate limit: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> Dict[Any, Any]:
        """
        Make HTTP request with retry logic.
        
        Args:
            method: HTTP method (GET, POST)
            url: Request URL
            **kwargs: Additional request parameters
        
        Returns:
            JSON response as dict
        
        Raises:
            APIError: On request failure
        """
        
        kwargs.setdefault('headers', API_HEADERS)
        kwargs.setdefault('timeout', API_TIMEOUT)
        
        for attempt in range(RETRY_POLICY['max_attempts']):
            try:
                self._rate_limit()
                
                response = self.session.request(method, url, **kwargs)
                
                if response.status_code == 200:
                    return response.json()
                
                elif response.status_code == 404:
                    raise CompanyNotFoundError(f"Not found: {url}")
                
                elif response.status_code == 429:
                    delay = RATE_LIMIT['retry_429_delay']
                    logger.warning(
                        f"Rate limit hit (429), waiting {delay}s"
                    )
                    time.sleep(delay)
                    continue
                
                elif response.status_code in RETRY_POLICY['retry_on_status']:
                    if attempt < RETRY_POLICY['max_attempts'] - 1:
                        delay = RETRY_POLICY['backoff_delays'][attempt]
                        logger.warning(
                            f"HTTP {response.status_code}, "
                            f"retry {attempt + 1}/{RETRY_POLICY['max_attempts']} "
                            f"in {delay}s"
                        )
                        time.sleep(delay)
                        continue
                    else:
                        raise APIError(
                            f"HTTP {response.status_code}: {response.text}"
                        )
                
                else:
                    raise APIError(
                        f"HTTP {response.status_code}: {response.text}"
                    )
            
            except requests.Timeout:
                if attempt < RETRY_POLICY['max_attempts'] - 1:
                    delay = RETRY_POLICY['backoff_delays'][attempt]
                    logger.warning(
                        f"Timeout, retry {attempt + 1}/"
                        f"{RETRY_POLICY['max_attempts']} in {delay}s"
                    )
                    time.sleep(delay)
                    continue
                else:
                    raise APIError("Request timeout")
            
            except requests.RequestException as e:
                raise APIError(f"Request failed: {e}")
        
        raise APIError(
            f"Failed after {RETRY_POLICY['max_attempts']} attempts"
        )
    
    def check_company_exists(self, bin_value: str) -> bool:
        """
        Check if company exists via search endpoint.
        
        Args:
            bin_value: BIN to check
        
        Returns:
            True if company exists, False otherwise
        """
        
        url = f"{self.base_url}{API_ENDPOINTS['search']}"
        
        payload = {
            "page": 1,
            "pageSize": 10,
            "text": bin_value,
            "market": {"value": None},
            "tax": {"value": None},
            "krp": [],
            "oked": [],
            "kato": []
        }
        
        try:
            response = self._make_request('POST', url, json=payload)
            
            total = response.get('total', 0)
            results = response.get('results', [])
            
            exists = total > 0 and len(results) > 0
            
            if exists:
                logger.debug(f"BIN {bin_value} exists in API")
            else:
                logger.debug(f"BIN {bin_value} not found in API")
            
            return exists
            
        except CompanyNotFoundError:
            return False
        except APIError as e:
            logger.error(f"Error checking BIN {bin_value}: {e}")
            raise
    
    def get_company_info(self, bin_value: str) -> Dict[Any, Any]:
        """
        Get full company information.
        
        Args:
            bin_value: Company BIN
        
        Returns:
            Company data as dict
        
        Raises:
            CompanyNotFoundError: If company not found
            APIError: On request failure
        """
        
        url = f"{self.base_url}{API_ENDPOINTS['info']}"
        params = {'id': bin_value, 'lang': 'ru'}
        
        try:
            response = self._make_request('GET', url, params=params)
            logger.debug(f"Fetched info for BIN {bin_value}")
            return response
            
        except CompanyNotFoundError:
            logger.warning(f"Company not found: {bin_value}")
            raise
        except APIError as e:
            logger.error(f"Error fetching info for {bin_value}: {e}")
            raise