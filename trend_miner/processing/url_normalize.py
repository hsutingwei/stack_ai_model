"""
URL normalization with deterministic domain parsing

使用 tldextract 確保 publisher_domain 解析的穩定性與一致性。
"""

from urllib.parse import urlparse, parse_qs, urlunparse
import tldextract
from typing import Optional


# 固定 tldextract extraction (確保 determinsm)
_EXTRACTOR = None


def get_extractor(cache_dir: Optional[str] = None):
    """取得 tldextract extractor (使用 cache 確保 determinism)"""
    global _EXTRACTOR
    if _EXTRACTOR is None:
        if cache_dir:
            _EXTRACTOR = tldextract.TLDExtract(cache_dir=cache_dir)
        else:
            # 使用內建 snapshot (不需要網路)
            _EXTRACTOR = tldextract.TLDExtract(suffix_list_urls=None)
    return _EXTRACTOR


def normalize_url(url: str, remove_params: bool = True) -> str:
    """
    正規化 URL
    
    1. 移除追蹤參數 (utm_*, fbclid, gclid, etc.)
    2. 移除 fragment (#xxx)
    3. Lowercase scheme/domain
    
    Args:
        url: 原始 URL
        remove_params: 是否移除追蹤參數
    
    Returns:
        正規化後的 canonical URL
    """
    parsed = urlparse(url)
    
    # Scheme/Domain lowercase
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    
    # 移除追蹤參數
    if remove_params and parsed.query:
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        
        # 追蹤參數清單
        tracking_params = {
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'fbclid', 'gclid', 'msclkid', '_ga', 'mc_cid', 'mc_eid',
            'ref', 'source', 'campaign_id', 'ad_id'
        }
        
        # 保留非追蹤參數
        clean_params = {k: v for k, v in query_params.items() 
                       if k.lower() not in tracking_params}
        
        # 重建 query string
        if clean_params:
            query_str = '&'.join(f"{k}={v[0]}" for k, v in sorted(clean_params.items()))
        else:
            query_str = ''
    else:
        query_str = parsed.query
    
    # 移除 fragment
    fragment = ''
    
    # 重建 URL
    canonical = urlunparse((scheme, netloc, parsed.path, parsed.params, query_str, fragment))
    return canonical


def extract_domain(url: str, cache_dir: Optional[str] = None) -> str:
    """
    提取 publisher domain (eTLD+1, 去 www)
    
    例如:
    - https://www.reuters.com/article/... -> reuters.com
    - https://finance.yahoo.com/news/... -> yahoo.com
    
    Args:
        url: URL
        cache_dir: tldextract cache 目錄
    
    Returns:
        Publisher domain (e.g. reuters.com)
    """
    extractor = get_extractor(cache_dir)
    extracted = extractor(url)
    
    # eTLD+1 (domain + suffix)
    domain = f"{extracted.domain}.{extracted.suffix}"
    
    # 去除 www
    if domain.startswith('www.'):
        domain = domain[4:]
    
    return domain.lower()
