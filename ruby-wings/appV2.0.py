# app.py — RUBY WINGS CHATBOT v3.0
# Tích hợp 10 upgrades với feature flags
# =========== CHẠY NGAY KHÔNG CẦN CHỈNH SỬA ===========

import os
import json
import threading
import logging
import re
import unicodedata
import traceback
import hashlib
import time
from functools import lru_cache
from typing import List, Tuple, Dict, Optional, Any
from datetime import datetime
from difflib import SequenceMatcher

from flask import Flask, request, jsonify
from flask_cors import CORS

import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound

# Meta CAPI
try:
    from meta_capi import send_meta_pageview, send_meta_lead
    HAS_META_CAPI = True
except ImportError:
    HAS_META_CAPI = False

# Try FAISS
HAS_FAISS = False
try:
    import faiss
    HAS_FAISS = True
except ImportError:
    HAS_FAISS = False

# OpenAI API
try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False
    OpenAI = None

# =========== CONFIGURATION UPGRADES ===========
# Tất cả upgrades có thể bật/tắt bằng biến môi trường
UPGRADE_1_MANDATORY_FILTER = os.environ.get("UPGRADE_1_MANDATORY_FILTER", "true").lower() == "true"
UPGRADE_2_DEDUPLICATION = os.environ.get("UPGRADE_2_DEDUPLICATION", "true").lower() == "true"
UPGRADE_3_ENHANCED_FIELDS = os.environ.get("UPGRADE_3_ENHANCED_FIELDS", "true").lower() == "true"
UPGRADE_4_QUESTION_PIPELINE = os.environ.get("UPGRADE_4_QUESTION_PIPELINE", "true").lower() == "true"
UPGRADE_5_QUERY_SPLITTER = os.environ.get("UPGRADE_5_QUERY_SPLITTER", "false").lower() == "true"
UPGRADE_6_FUZZY_MATCHING = os.environ.get("UPGRADE_6_FUZZY_MATCHING", "true").lower() == "true"
UPGRADE_7_STATE_MACHINE = os.environ.get("UPGRADE_7_STATE_MACHINE", "false").lower() == "true"
UPGRADE_8_SEMANTIC_ANALYSIS = os.environ.get("UPGRADE_8_SEMANTIC_ANALYSIS", "false").lower() == "true"
UPGRADE_9_AUTO_VALIDATION = os.environ.get("UPGRADE_9_AUTO_VALIDATION", "true").lower() == "true"
UPGRADE_10_TEMPLATE_SYSTEM = os.environ.get("UPGRADE_10_TEMPLATE_SYSTEM", "true").lower() == "true"

# =========== LOGGING ===========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("rbw_v3")

# =========== ENVIRONMENT VARIABLES ===========
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

KNOWLEDGE_PATH = os.environ.get("KNOWLEDGE_PATH", "knowledge.json")
FAISS_INDEX_PATH = os.environ.get("FAISS_INDEX_PATH", "faiss_index.bin")
FAISS_MAPPING_PATH = os.environ.get("FAISS_MAPPING_PATH", "faiss_mapping.json")
FALLBACK_VECTORS_PATH = os.environ.get("FALLBACK_VECTORS_PATH", "vectors.npz")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
CHAT_MODEL = os.environ.get("CHAT_MODEL", "gpt-4o-mini")
TOP_K = int(os.environ.get("TOP_K", "5"))
FAISS_ENABLED = os.environ.get("FAISS_ENABLED", "true").lower() in ("1", "true", "yes")

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "1SdVbwkuxb8l1meEW--ddyfh4WmUvSXXMOPQ5bCyPkdk")
GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "RBW_Lead_Raw_Inbox")

ENABLE_GOOGLE_SHEETS = os.environ.get("ENABLE_GOOGLE_SHEETS", "true").lower() in ("1", "true", "yes")
ENABLE_FALLBACK_STORAGE = os.environ.get("ENABLE_FALLBACK_STORAGE", "true").lower() in ("1", "true", "yes")
FALLBACK_STORAGE_PATH = os.environ.get("FALLBACK_STORAGE_PATH", "leads_fallback.json")
ENABLE_META_CAPI_CALL = os.environ.get("ENABLE_META_CAPI_CALL", "true").lower() in ("1", "true", "yes")

META_CAPI_ENDPOINT = os.environ.get("META_CAPI_ENDPOINT", "https://graph.facebook.com/v17.0/")
META_CAPI_TOKEN = os.environ.get("META_CAPI_TOKEN", "").strip()
META_PIXEL_ID = os.environ.get("META_PIXEL_ID", "").strip()
META_TEST_EVENT_CODE = os.environ.get("META_TEST_EVENT_CODE", "").strip()

FLASK_ENV = os.environ.get("FLASK_ENV", "production")
DEBUG = os.environ.get("DEBUG", "false").lower() in ("1", "true", "yes")
SECRET_KEY = os.environ.get("SECRET_KEY", "ruby-wings-secret-key-2024")
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "https://www.rubywings.vn,http://localhost:3000").split(",")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "10000"))

# =========== UPGRADE 1: MANDATORY FILTER SYSTEM ===========
class MandatoryFilter:
    """Lọc bắt buộc trước khi xử lý"""
    
    FILTER_PATTERNS = {
        'duration': [
            (r'1\s*ngày', '1 ngày'),
            (r'2\s*ngày', '2 ngày'),
            (r'3\s*ngày', '3 ngày'),
            (r'1\s*ngày\s*1\s*đêm', '1 ngày 1 đêm'),
            (r'2\s*ngày\s*1\s*đêm', '2 ngày 1 đêm')
        ],
        'price': [
            (r'dưới\s*(\d+)\s*(triệu|tr|k)', 'max_price'),
            (r'khoảng\s*(\d+)\s*(triệu|tr)', 'approx_price'),
            (r'trên\s*(\d+)\s*(triệu|tr)', 'min_price')
        ],
        'location': [
            (r'(?:ở|tại|về)\s+(.*?)(?:\s|$|\.|,)', 'location')
        ]
    }
    
    @staticmethod
    def extract_filters(message: str) -> Dict:
        """Trích xuất điều kiện bắt buộc từ message"""
        filters = {}
        message_lower = message.lower()
        
        # Duration filters
        for pattern, label in MandatoryFilter.FILTER_PATTERNS['duration']:
            if re.search(pattern, message_lower):
                filters['duration'] = label
                break
        
        # Price filters
        for pattern, filter_type in MandatoryFilter.FILTER_PATTERNS['price']:
            match = re.search(pattern, message_lower)
            if match:
                try:
                    value = int(match.group(1))
                    if 'triệu' in match.group(2) or 'tr' in match.group(2):
                        value = value * 1000000
                    filters[filter_type] = value
                except:
                    pass
        
        # Location filters
        for pattern, filter_type in MandatoryFilter.FILTER_PATTERNS['location']:
            match = re.search(pattern, message_lower)
            if match:
                filters['location'] = match.group(1).strip()
                break
        
        return filters
    
    @staticmethod
    def filter_tours(tours_db: Dict, filters: Dict) -> List[int]:
        """Lọc tour theo điều kiện bắt buộc"""
        if not filters or not tours_db:
            return list(tours_db.keys())
        
        filtered_indices = []
        
        for tour_idx, tour_data in tours_db.items():
            passes = True
            
            # Check duration
            if 'duration' in filters and 'duration' in tour_data:
                tour_duration = tour_data['duration'].lower()
                filter_duration = filters['duration'].lower()
                if filter_duration not in tour_duration:
                    passes = False
            
            # Check location
            if passes and 'location' in filters and 'location' in tour_data:
                tour_location = tour_data['location'].lower()
                filter_location = filters['location'].lower()
                if filter_location not in tour_location:
                    passes = False
            
            # Check price
            if passes and ('max_price' in filters or 'min_price' in filters) and 'price' in tour_data:
                price_text = tour_data['price']
                price_numbers = re.findall(r'[\d,\.]+', price_text)
                if price_numbers:
                    try:
                        # Lấy giá đầu tiên
                        first_price = price_numbers[0].replace(',', '').replace('.', '')
                        if first_price.isdigit():
                            price_val = int(first_price)
                            
                            if 'max_price' in filters and price_val > filters['max_price']:
                                passes = False
                            if 'min_price' in filters and price_val < filters['min_price']:
                                passes = False
                    except:
                        pass
            
            if passes:
                filtered_indices.append(tour_idx)
        
        return filtered_indices

# =========== UPGRADE 2: DEDUPLICATION ENGINE ===========
class DeduplicationEngine:
    """Xử lý trùng lặp trong kết quả"""
    
    @staticmethod
    def similarity(text1: str, text2: str) -> float:
        """Tính độ tương đồng giữa 2 văn bản"""
        if not text1 or not text2:
            return 0.0
        
        # Normalize text
        text1 = text1.lower().strip()
        text2 = text2.lower().strip()
        
        # Simple word overlap
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        jaccard = len(intersection) / len(union) if union else 0.0
        
        # Sequence similarity
        seq_similarity = SequenceMatcher(None, text1, text2).ratio()
        
        # Combined score
        return (jaccard * 0.4) + (seq_similarity * 0.6)
    
    @staticmethod
    def deduplicate_passages(passages: List[Tuple[float, dict]], threshold: float = 0.8) -> List[Tuple[float, dict]]:
        """Loại bỏ các passage trùng lặp"""
        if len(passages) <= 1:
            return passages
        
        unique_passages = []
        seen_texts = []
        
        for score, passage in sorted(passages, key=lambda x: x[0], reverse=True):
            text = passage.get('text', '').strip()
            path = passage.get('path', '')
            
            # Check if similar text already seen
            is_duplicate = False
            for seen_text in seen_texts:
                if DeduplicationEngine.similarity(text, seen_text) > threshold:
                    is_duplicate = True
                    break
            
            if not is_duplicate and text:
                unique_passages.append((score, passage))
                seen_texts.append(text[:100])  # Store first 100 chars
        
        return unique_passages

# =========== UPGRADE 3: ENHANCED FIELD DETECTION ===========
class EnhancedFieldDetector:
    """Phát hiện field nâng cao"""
    
    EXTENDED_KEYWORD_MAP = {
        "tour_list": {
            "keywords": [
                "tên tour", "tour gì", "danh sách tour", "có những tour nào", 
                "liệt kê tour", "show tour", "tour hiện có", "tour available",
                "liệt kê các tour đang có", "list tour", "tour đang bán", 
                "tour hiện hành", "tour nào", "tours", "liệt kê các tour",
                "liệt kê các hành trình", "list tours", "show tours",
                "các tour hiện tại", "kể tên tour", "tour của bạn có"
            ],
            "field": "tour_name"
        },
        "price": {
            "keywords": [
                "giá tour", "chi phí", "bao nhiêu tiền", "price", "cost",
                "giá cả", "bao nhiêu", "phí", "kinh phí", "tổng chi phí",
                "tiền tour", "giá vé", "vé bao nhiêu", "đóng tiền"
            ],
            "field": "price"
        },
        "duration": {
            "keywords": [
                "thời gian tour", "kéo dài", "mấy ngày", "bao lâu", 
                "ngày đêm", "duration", "tour dài bao lâu", 
                "tour bao nhiêu ngày", "2 ngày 1 đêm", "3 ngày 2 đêm",
                "trong bao lâu", "thời lượng", "mấy đêm", "bao nhiêu ngày"
            ],
            "field": "duration"
        },
        "location": {
            "keywords": [
                "ở đâu", "đi đâu", "địa phương nào", "nơi nào", 
                "điểm đến", "destination", "location", "địa điểm",
                "tới đâu", "thăm quan đâu", "khám phá đâu"
            ],
            "field": "location"
        },
        "includes": {
            "keywords": [
                "lịch trình chi tiết", "chương trình chi tiết", 
                "chi tiết hành trình", "itinerary", "schedule", "includes",
                "làm gì", "hoạt động", "chương trình", "lịch trình",
                "hành trình", "sinh hoạt", "hoạt động gì"
            ],
            "field": "includes"
        },
        "accommodation": {
            "keywords": [
                "chỗ ở", "nơi lưu trú", "khách sạn", "homestay", 
                "accommodation", "nghỉ ở đâu", "ở khách sạn nào",
                "chỗ ngủ", "phòng ở", "nơi nghỉ"
            ],
            "field": "accommodation"
        },
        "meals": {
            "keywords": [
                "ăn uống", "ẩm thực", "meals", "thực đơn", "bữa",
                "đồ ăn", "thức ăn", "ăn gì", "uống gì", "bữa ăn",
                "ẩm thực gì", "đặc sản", "món ăn"
            ],
            "field": "meals"
        },
        "summary": {
            "keywords": [
                "tóm tắt chương trình tour", "tóm tắt", "overview", 
                "brief", "mô tả ngắn", "giới thiệu", "tổng quan",
                "mô tả tour", "tour này thế nào", "có gì hay"
            ],
            "field": "summary"
        },
        "transport": {
            "keywords": [
                "vận chuyển", "phương tiện", "di chuyển", "xe gì", 
                "transportation", "đi bằng gì", "xe cộ", "phương tiện gì"
            ],
            "field": "transport"
        }
    }
    
    @staticmethod
    def detect_field(message: str) -> Tuple[Optional[str], float]:
        """Phát hiện field với confidence score"""
        if not message:
            return None, 0.0
        
        message_lower = message.lower()
        best_field = None
        best_score = 0.0
        
        for field_info in EnhancedFieldDetector.EXTENDED_KEYWORD_MAP.values():
            field_name = field_info["field"]
            keywords = field_info["keywords"]
            
            score = 0.0
            for keyword in keywords:
                if keyword in message_lower:
                    # Tính điểm dựa trên độ dài keyword và vị trí
                    keyword_len = len(keyword)
                    position = message_lower.find(keyword)
                    
                    # Keyword càng dài, điểm càng cao
                    # Keyword xuất hiện sớm, điểm càng cao
                    keyword_score = (keyword_len / 20) * (1.0 - (position / len(message_lower)))
                    score = max(score, keyword_score)
            
            if score > best_score:
                best_score = score
                best_field = field_name
        
        return best_field, best_score

# =========== UPGRADE 4: QUESTION PIPELINE ===========
class QuestionPipeline:
    """Pipeline xử lý các loại câu hỏi"""
    
    @staticmethod
    def classify_question(message: str) -> str:
        """Phân loại câu hỏi"""
        message_lower = message.lower()
        
        # Comparison questions
        comparison_keywords = ["so sánh", "sánh", "compare", "khác nhau", "giống nhau", "hơn kém"]
        if any(keyword in message_lower for keyword in comparison_keywords):
            return "comparison"
        
        # Recommendation questions
        recommendation_keywords = ["phù hợp", "recommend", "gợi ý", "nên chọn", "tư vấn", "tốt nhất", "nên đi"]
        if any(keyword in message_lower for keyword in recommendation_keywords):
            return "recommendation"
        
        # List questions
        list_keywords = ["liệt kê", "danh sách", "có những", "kể tên", "list", "show", "các"]
        if any(keyword in message_lower for keyword in list_keywords):
            return "list"
        
        # Calculation questions
        calculation_keywords = ["tính", "tổng", "cộng", "bao nhiêu", "mấy", "số lượng"]
        if any(keyword in message_lower and "tour" in message_lower for keyword in calculation_keywords):
            return "calculation"
        
        # Default: information request
        return "information"
    
    @staticmethod
    def process_comparison(tour_indices: List[int], tours_db: Dict, aspect: str = "") -> str:
        """Xử lý câu hỏi so sánh"""
        if len(tour_indices) < 2:
            return "Cần ít nhất 2 tour để so sánh."
        
        result = ["**SO SÁNH TOUR:**\n"]
        
        # Lấy thông tin các tour
        tours_info = []
        for idx in tour_indices[:3]:  # Giới hạn 3 tour
            if idx in tours_db:
                tours_info.append((idx, tours_db[idx]))
        
        if len(tours_info) < 2:
            return "Không đủ thông tin để so sánh."
        
        # Tạo bảng so sánh
        headers = ["Tiêu chí"]
        for idx, tour in tours_info:
            tour_name = tour.get("tour_name", f"Tour #{idx}")
            headers.append(tour_name[:20])  # Giới hạn độ dài
        
        result.append("| " + " | ".join(headers) + " |")
        result.append("|" + "---|" * len(headers))
        
        # Các tiêu chí so sánh
        comparison_fields = ["duration", "location", "price"]
        if aspect:
            # Ưu tiên aspect được yêu cầu
            comparison_fields = [aspect] + [f for f in comparison_fields if f != aspect]
        
        for field in comparison_fields:
            row = [field.capitalize()]
            for idx, tour in tours_info:
                value = tour.get(field, "N/A")
                if isinstance(value, list):
                    value = ", ".join(value[:2])
                row.append(str(value)[:30])  # Giới hạn độ dài
            result.append("| " + " | ".join(row) + " |")
        
        # Thêm kết luận
        result.append("\n**Kết luận:**")
        if "duration" in comparison_fields:
            durations = [tour.get("duration", "") for _, tour in tours_info]
            if all("1 ngày" in d for d in durations if d):
                result.append("- Tất cả đều là tour 1 ngày, phù hợp cho người bận rộn.")
            elif any("2 ngày" in d for d in durations if d):
                result.append("- Có tour 2 ngày, phù hợp cho trải nghiệm sâu hơn.")
        
        return "\n".join(result)
    
    @staticmethod
    def process_recommendation(user_prefs: Dict, tours_db: Dict) -> str:
        """Xử lý đề xuất tour"""
        if not tours_db:
            return "Hiện chưa có đủ dữ liệu tour để đề xuất."
        
        # Tính điểm cho mỗi tour
        recommendations = []
        for idx, tour in tours_db.items():
            score = 0.0
            reasons = []
            
            # Kiểm tra duration preference
            if user_prefs.get("duration_pref") and "duration" in tour:
                tour_duration = tour["duration"].lower()
                user_duration = user_prefs["duration_pref"]
                
                if user_duration == "1day" and ("1 ngày" in tour_duration):
                    score += 2.0
                    reasons.append("phù hợp thời gian 1 ngày")
                elif user_duration == "2day" and ("2 ngày" in tour_duration):
                    score += 2.0
                    reasons.append("phù hợp thời gian 2 ngày")
            
            # Kiểm tra location preference
            if user_prefs.get("location_pref") and "location" in tour:
                tour_location = tour["location"].lower()
                user_location = user_prefs["location_pref"].lower()
                
                if user_location in tour_location:
                    score += 1.5
                    reasons.append(f"có địa điểm {user_location}")
            
            # Kiểm tra interests
            if user_prefs.get("interests") and "summary" in tour:
                tour_summary = tour["summary"].lower()
                for interest in user_prefs["interests"]:
                    if interest in tour_summary:
                        score += 1.0
                        reasons.append(f"có yếu tố {interest}")
            
            if score > 0:
                recommendations.append({
                    "idx": idx,
                    "tour": tour,
                    "score": score,
                    "reasons": reasons
                })
        
        # Sắp xếp theo điểm
        recommendations.sort(key=lambda x: x["score"], reverse=True)
        
        # Format kết quả
        if not recommendations:
            # Fallback: top 3 tours
            top_tours = list(tours_db.items())[:3]
            result = ["**TOP 3 TOUR PHỔ BIẾN:**\n"]
            for idx, tour in top_tours:
                name = tour.get("tour_name", f"Tour #{idx}")
                duration = tour.get("duration", "")
                location = tour.get("location", "")
                result.append(f"**• {name}**")
                if duration:
                    result.append(f"  Thời gian: {duration}")
                if location:
                    result.append(f"  Địa điểm: {location}")
                result.append("")
            return "\n".join(result)
        
        result = ["**ĐỀ XUẤT DỰA TRÊN SỞ THÍCH CỦA BẠN:**\n"]
        
        for i, rec in enumerate(recommendations[:3], 1):
            tour = rec["tour"]
            name = tour.get("tour_name", f"Tour #{rec['idx']}")
            duration = tour.get("duration", "")
            location = tour.get("location", "")
            price = tour.get("price", "")
            
            result.append(f"**{i}. {name}** ⭐ ({rec['score']:.1f}/5)")
            if duration:
                result.append(f"   🕒 {duration}")
            if location:
                result.append(f"   📍 {location}")
            if price:
                result.append(f"   💰 {price}")
            
            if rec["reasons"]:
                result.append(f"   ✅ " + ", ".join(rec["reasons"]))
            
            result.append("")
        
        return "\n".join(result)

# =========== UPGRADE 6: FUZZY MATCHING ===========
class FuzzyMatcher:
    """Fuzzy matching cho tên tour"""
    
    @staticmethod
    def normalize_name(name: str) -> str:
        """Chuẩn hóa tên tour cho matching"""
        if not name:
            return ""
        
        # Lowercase và remove diacritics
        name = name.lower()
        name = unicodedata.normalize('NFD', name)
        name = ''.join(ch for ch in name if unicodedata.category(ch) != 'Mn')
        
        # Remove punctuation và extra spaces
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        # Remove common words
        stop_words = ['tour', 'travel', 'du lịch', 'hành trình', 'trải nghiệm']
        words = [w for w in name.split() if w not in stop_words]
        
        return ' '.join(words)
    
    @staticmethod
    def find_best_match(query: str, tour_names: Dict[str, int], threshold: float = 0.7) -> List[int]:
        """Tìm tour phù hợp nhất với tên query"""
        if not query or not tour_names:
            return []
        
        query_norm = FuzzyMatcher.normalize_name(query)
        if not query_norm:
            return []
        
        matches = []
        
        for tour_name, tour_idx in tour_names.items():
            tour_norm = FuzzyMatcher.normalize_name(tour_name)
            if not tour_norm:
                continue
            
            # Tính similarity
            similarity = SequenceMatcher(None, query_norm, tour_norm).ratio()
            
            # Kiểm tra từ khóa quan trọng
            query_words = set(query_norm.split())
            tour_words = set(tour_norm.split())
            
            # Có từ khóa chung
            common_words = query_words.intersection(tour_words)
            if common_words:
                similarity += len(common_words) * 0.1
            
            if similarity >= threshold:
                matches.append((similarity, tour_idx))
        
        # Sắp xếp theo similarity
        matches.sort(reverse=True)
        
        # Lấy các tour có similarity cao nhất
        if matches:
            max_similarity = matches[0][0]
            best_matches = [idx for sim, idx in matches if sim >= max_similarity * 0.9]
            return best_matches[:3]  # Giới hạn 3 tour
        
        return []

# =========== UPGRADE 9: AUTO VALIDATION ===========
class AutoValidator:
    """Tự động validate thông tin trong response"""
    
    @staticmethod
    def validate_duration(text: str) -> str:
        """Validate và sửa thông tin thời gian không hợp lý"""
        patterns = [
            r'(\d+)\s*ngày\s*(\d+)\s*đêm',
            r'(\d+)\s*ngày',
            r'(\d+)\s*đêm'
        ]
        
        for pattern in patterns:
            matches = list(re.finditer(pattern, text))
            for match in matches:
                try:
                    if match.lastindex == 2:  # "X ngày Y đêm"
                        days = int(match.group(1))
                        nights = int(match.group(2))
                        
                        # Kiểm tra tính hợp lý
                        if days > 7 or nights > 7:
                            # Thay thế bằng thông tin chung
                            text = text.replace(match.group(0), "thời gian phù hợp")
                        elif abs(days - nights) > 1:
                            # Điều chỉnh cho hợp lý
                            if days > nights:
                                new_duration = f"{nights} ngày {nights} đêm"
                            else:
                                new_duration = f"{days} ngày {days} đêm"
                            text = text.replace(match.group(0), new_duration)
                    
                    elif match.lastindex == 1:  # "X ngày" hoặc "Y đêm"
                        num = int(match.group(1))
                        if num > 7:  # Quá dài
                            text = text.replace(match.group(0), "thời gian phù hợp")
                            
                except (ValueError, IndexError):
                    continue
        
        return text
    
    @staticmethod
    def validate_price(text: str) -> str:
        """Validate và sửa thông tin giá không hợp lý"""
        # Tìm các số tiền
        price_patterns = [
            r'(\d[\d,\.]*)\s*(triệu|tr|k|nghìn|đồng|vnđ|vnd)',
            r'(\d[\d,\.]*)\s*-\s*(\d[\d,\.]*)'
        ]
        
        for pattern in price_patterns:
            matches = list(re.finditer(pattern, text, re.IGNORECASE))
            for match in matches:
                try:
                    if match.lastindex == 2:
                        # "X triệu" hoặc "X - Y"
                        if match.group(2).lower() in ['triệu', 'tr']:
                            # Chuyển triệu sang số
                            value = float(match.group(1).replace(',', '').replace('.', ''))
                            if value > 50:  # 50 triệu là quá cao cho tour
                                text = text.replace(match.group(0), "giá hợp lý")
                    
                except (ValueError, AttributeError):
                    continue
        
        return text
    
    @staticmethod
    def validate_response(response: str) -> str:
        """Validate toàn bộ response"""
        if not response:
            return response
        
        # Validate duration
        response = AutoValidator.validate_duration(response)
        
        # Validate price
        response = AutoValidator.validate_price(response)
        
        # Kiểm tra các từ không hợp lý
        invalid_phrases = [
            "không biết", "không rõ", "không có thông tin", 
            "chưa cập nhật", "đang cập nhật"
        ]
        
        for phrase in invalid_phrases:
            if phrase in response.lower():
                # Thay thế bằng thông tin chung
                if "tour" in response.lower():
                    response += "\n\n*Vui lòng liên hệ hotline 0332510486 để biết thông tin chi tiết nhất.*"
        
        return response

# =========== UPGRADE 10: TEMPLATE SYSTEM ===========
class TemplateSystem:
    """Hệ thống template cho response"""
    
    TEMPLATES = {
        "tour_list": """
✨ **DANH SÁCH TOUR RUBY WINGS** ✨

{tour_items}

📞 **Liên hệ đặt tour:** 0332510486
📍 **Địa chỉ:** Ruby Wings Travel
""",
        
        "tour_list_item": """
**{index}. {tour_name}** {duration_emoji}
   ⏱️ Thời gian: {duration}
   📍 Địa điểm: {location}
   💰 Giá: {price}
   📝 {summary}
""",
        
        "tour_detail": """
🎯 **{tour_name}**

📋 **Thông tin chính:**
   ⏱️ Thời gian: {duration}
   📍 Địa điểm: {location}
   💰 Giá: {price}

📖 **Mô tả:**
{summary}

🏨 **Chỗ ở:** {accommodation}
🍽️ **Ăn uống:** {meals}
🚗 **Di chuyển:** {transport}

📞 **Đặt tour ngay:** 0332510486
""",
        
        "comparison": """
📊 **SO SÁNH TOUR**

{comparison_table}

💡 **Gợi ý lựa chọn:**
{recommendations}

📞 **Tư vấn thêm:** 0332510486
""",
        
        "recommendation": """
🎯 **ĐỀ XUẤT TOUR PHÙ HỢP**

{recommendation_items}

📋 **Tiêu chí đề xuất:**
{criteria}

📞 **Liên hệ tư vấn:** 0332510486
"""
    }
    
    @staticmethod
    def render(template_name: str, **kwargs) -> str:
        """Render template với variables"""
        template = TemplateSystem.TEMPLATES.get(template_name, "{content}")
        
        # Replace variables
        for key, value in kwargs.items():
            placeholder = "{" + key + "}"
            if placeholder in template:
                template = template.replace(placeholder, str(value) if value else "Đang cập nhật")
        
        # Clean up empty lines
        lines = template.split('\n')
        cleaned_lines = []
        for line in lines:
            if line.strip() or line == "":
                cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)

# =========== GLOBAL STATE ===========
app = Flask(__name__)
CORS(app, origins=CORS_ORIGINS)

# Initialize OpenAI client
client = None
if HAS_OPENAI and OPENAI_API_KEY:
    try:
        client = OpenAI(api_key=OPENAI_API_KEY, timeout=15)
        logger.info("OpenAI client initialized")
    except Exception as e:
        logger.error(f"OpenAI init failed: {e}")
        client = None

# Knowledge base state
KNOW: Dict = {}
FLAT_TEXTS: List[str] = []
MAPPING: List[dict] = []
INDEX = None
INDEX_LOCK = threading.Lock()
TOUR_NAME_TO_INDEX: Dict[str, int] = {}
TOURS_DB: Dict[int, Dict[str, Any]] = {}
TOUR_TAGS: Dict[int, List[str]] = {}

# Google Sheets client cache
_gsheet_client = None
_gsheet_client_lock = threading.Lock()

# Fallback storage for leads
_fallback_storage_lock = threading.Lock()

# =========== ENHANCED CONTEXT (GIỮ NGUYÊN TỪ APP.PY) ===========
class EnhancedContext:
    def __init__(self):
        self.tour_mentions = []
        self.user_preferences = {
            "duration_pref": None,
            "price_range": None,
            "interests": [],
            "location_pref": None
        }
        self.conversation_stack = []
        self.last_action = None
        self.timestamp = datetime.utcnow()
        self.last_tour_indices = []
        self.conversation_history = []
        self.last_tour_name = None

ENHANCED_SESSION_CONTEXT = {}
CONTEXT_TIMEOUT = 1800

def cleanup_old_contexts():
    now = datetime.utcnow()
    to_delete = []
    for session_id, context in ENHANCED_SESSION_CONTEXT.items():
        if (now - context.timestamp).total_seconds() > CONTEXT_TIMEOUT:
            to_delete.append(session_id)
    for session_id in to_delete:
        del ENHANCED_SESSION_CONTEXT[session_id]

def get_session_context(session_id):
    cleanup_old_contexts()
    if session_id not in ENHANCED_SESSION_CONTEXT:
        ENHANCED_SESSION_CONTEXT[session_id] = EnhancedContext()
    return ENHANCED_SESSION_CONTEXT[session_id]

def update_tour_context(session_id, tour_indices, tour_name=None):
    context = get_session_context(session_id)
    if tour_indices:
        context.last_tour_indices = tour_indices
        for idx in tour_indices:
            context.tour_mentions.append((idx, 1.0, datetime.utcnow()))
        if len(context.tour_mentions) > 5:
            context.tour_mentions = context.tour_mentions[-5:]
    if tour_name:
        context.last_tour_name = tour_name
    context.timestamp = datetime.utcnow()
    return context

def extract_session_id(request_data, remote_addr):
    session_id = request_data.get("session_id")
    if not session_id:
        ip = remote_addr or "0.0.0.0"
        current_hour = datetime.utcnow().strftime("%Y%m%d%H")
        unique_str = f"{ip}_{current_hour}"
        session_id = hashlib.md5(unique_str.encode()).hexdigest()[:12]
    return f"session_{session_id}"

# =========== KEYWORD MAPPING (GIỮ NGUYÊN) ===========
KEYWORD_FIELD_MAP: Dict[str, Dict] = {
    "tour_list": {
        "keywords": [
            "tên tour", "tour gì", "danh sách tour", "có những tour nào", "liệt kê tour",
            "show tour", "tour hiện có", "tour available", "liệt kê các tour đang có",
            "list tour", "tour đang bán", "tour hiện hành", "tour nào", "tours", "liệt kê các tour",
            "liệt kê các hành trình", "list tours", "show tours", "các tour hiện tại"
        ],
        "field": "tour_name"
    },
    "mission": {"keywords": ["tầm nhìn", "sứ mệnh", "giá trị cốt lõi", "triết lý", "vision", "mission"], "field": "mission"},
    "summary": {"keywords": ["tóm tắt chương trình tour", "tóm tắt", "overview", "brief", "mô tả ngắn"], "field": "summary"},
    "style": {"keywords": ["phong cách hành trình", "tính chất hành trình", "concept tour", "vibe tour", "style"], "field": "style"},
    "transport": {"keywords": ["vận chuyển", "phương tiện", "di chuyển", "xe gì", "transportation"], "field": "transport"},
    "includes": {"keywords": ["lịch trình chi tiết", "chương trình chi tiết", "chi tiết hành trình", "itinerary", "schedule", "includes"], "field": "includes"},
    "location": {"keywords": ["ở đâu", "đi đâu", "địa phương nào", "nơi nào", "điểm đến", "destination", "location"], "field": "location"},
    "duration": {"keywords": ["thời gian tour", "kéo dài", "mấy ngày", "bao lâu", "ngày đêm", "duration", "tour dài bao lâu", "tour bao nhiêu ngày", "2 ngày 1 đêm", "3 ngày 2 đêm"], "field": "duration"},
    "price": {"keywords": ["giá tour", "chi phí", "bao nhiêu tiền", "price", "cost"], "field": "price"},
    "notes": {"keywords": ["lưu ý", "ghi chú", "notes", "cần chú ý"], "field": "notes"},
    "accommodation": {"keywords": ["chỗ ở", "nơi lưu trú", "khách sạn", "homestay", "accommodation"], "field": "accommodation"},
    "meals": {"keywords": ["ăn uống", "ẩm thực", "meals", "thực đơn", "bữa"], "field": "meals"},
    "event_support": {"keywords": ["hỗ trợ", "dịch vụ hỗ trợ", "event support", "dịch vụ tăng cường"], "field": "event_support"},
    "cancellation_policy": {"keywords": ["phí huỷ", "chính sách huỷ", "cancellation", "refund policy"], "field": "cancellation_policy"},
    "booking_method": {"keywords": ["đặt chỗ", "đặt tour", "booking", "cách đặt"], "field": "booking_method"},
    "who_can_join": {"keywords": ["phù hợp đối tượng", "ai tham gia", "who should join"], "field": "who_can_join"},
    "hotline": {"keywords": ["hotline", "số điện thoại", "liên hệ", "contact number"], "field": "hotline"},
}

# =========== FIELD INFERENCE RULES (GIỮ NGUYÊN) ===========
FIELD_INFERENCE_RULES = {
    "price": {
        "default_for_1day": "800.000 - 1.500.000 VNĐ",
        "default_for_2day": "1.500.000 - 2.500.000 VNĐ",
        "missing_response": "Liệt kê tất cả tour có giá trong database"
    },
    "meals": {
        "1day_tours": "Các tour 1 ngày đều bao gồm ít nhất 1 bữa chính",
        "2day_tours": "Tour 2 ngày thường bao gồm 3 bữa chính/ngày + 2 bữa sáng"
    },
    "duration": {
        "tour_reference": "Các tour thường có thời gian 1 ngày hoặc 2 ngày 1 đêm"
    },
    "accommodation": {
        "1day_tours": "Tour 1 ngày không bao gồm chỗ ở qua đêm",
        "2day_tours": "Tour 2 ngày 1 đêm bao gồm 1 đêm lưu trú"
    }
}

COMMON_SENSE_RULES = {
    "1day_tour_meals": "Tour 1 ngày luôn bao gồm ít nhất 1 bữa ăn (thường là bữa trưa).",
    "2day_tour_accommodation": "Tour 2 ngày 1 đêm luôn bao gồm chỗ ở qua đêm.",
    "tour_includes_transport": "Tất cả các tour đều bao gồm phương tiện di chuyển (xe du lịch).",
    "tour_has_price": "Mọi tour đều có giá cả cụ thể, có thể thay đổi theo số lượng người.",
    "tour_has_location": "Mỗi tour đều có địa điểm cụ thể để tham quan.",
    "basic_includes": "Các tour đều bao gồm hướng dẫn viên và bảo hiểm du lịch."
}

# =========== UTILITY FUNCTIONS ===========
def normalize_text_simple(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# =========== TOUR DATABASE FUNCTIONS ===========
def index_tour_names():
    global TOUR_NAME_TO_INDEX
    TOUR_NAME_TO_INDEX = {}
    for m in MAPPING:
        path = m.get("path", "")
        if path.endswith(".tour_name"):
            txt = m.get("text", "") or ""
            norm = normalize_text_simple(txt)
            if not norm:
                continue
            match = re.search(r"\[(\d+)\]", path)
            if match:
                idx = int(match.group(1))
                prev = TOUR_NAME_TO_INDEX.get(norm)
                if prev is None:
                    TOUR_NAME_TO_INDEX[norm] = idx

def build_tours_db():
    global TOURS_DB, TOUR_TAGS
    TOURS_DB.clear()
    TOUR_TAGS.clear()
    
    for m in MAPPING:
        path = m.get("path", "")
        text = m.get("text", "")
        if not path or not text:
            continue
        
        tour_match = re.search(r'tours\[(\d+)\]', path)
        if not tour_match:
            continue
            
        tour_idx = int(tour_match.group(1))
        field_match = re.search(r'tours\[\d+\]\.(\w+)(?:\[\d+\])?', path)
        if not field_match:
            continue
            
        field_name = field_match.group(1)
        
        if tour_idx not in TOURS_DB:
            TOURS_DB[tour_idx] = {}
        
        current_value = TOURS_DB[tour_idx].get(field_name)
        if current_value is None:
            TOURS_DB[tour_idx][field_name] = text
        elif isinstance(current_value, list):
            current_value.append(text)
        elif isinstance(current_value, str):
            TOURS_DB[tour_idx][field_name] = [current_value, text]
    
    # Generate tags for each tour
    for tour_idx, tour_data in TOURS_DB.items():
        tags = []
        
        location = tour_data.get("location", "")
        if location:
            locations = [loc.strip() for loc in location.split(",") if loc.strip()]
            tags.extend([f"location:{loc}" for loc in locations[:2]])
        
        duration = tour_data.get("duration", "")
        if duration:
            if "1 ngày" in duration or "1ngày" in duration:
                tags.append("duration:1day")
            elif "2 ngày" in duration or "2ngày" in duration:
                tags.append("duration:2day")
            elif "3 ngày" in duration or "3ngày" in duration:
                tags.append("duration:3day")
        
        tour_name = tour_data.get("tour_name", "")
        if tour_name:
            name_lower = tour_name.lower()
            if "bạch mã" in name_lower:
                tags.append("destination:bachma")
            if "trường sơn" in name_lower:
                tags.append("destination:truongson")
            if "quảng trị" in name_lower:
                tags.append("destination:quangtri")
            if "huế" in name_lower:
                tags.append("destination:hue")
        
        TOUR_TAGS[tour_idx] = list(set(tags))
    
    logger.info(f"Built tours database: {len(TOURS_DB)} tours")

def resolve_tour_reference(message: str, context) -> List[int]:
    if not message:
        return []
    
    text_l = message.lower().strip()
    msg_norm = normalize_text_simple(message)
    
    # 1. Direct tour name matches
    direct_matches = []
    for norm_name, idx in TOUR_NAME_TO_INDEX.items():
        tour_words = set(norm_name.split())
        msg_words = set(msg_norm.split())
        common_words = tour_words & msg_words
        if len(common_words) >= 1:
            score = len(common_words) / max(len(tour_words), 1)
            direct_matches.append((score, idx))
    
    if direct_matches:
        direct_matches.sort(reverse=True)
        best_score = direct_matches[0][0]
        if best_score >= 0.5:
            selected = [idx for score, idx in direct_matches if score == best_score]
            return sorted(set(selected))
    
    # 2. Feature matching (duration, location)
    feature_matches = []
    duration_filter = None
    if "1 ngày" in text_l or "1ngày" in text_l:
        duration_filter = "1 ngày"
    elif "2 ngày" in text_l or "2ngày" in text_l:
        duration_filter = "2 ngày"
    
    location_keywords = []
    for loc in ['quảng trị', 'huế', 'bạch mã', 'trường sơn', 'đông hà', 'khe sanh']:
        if loc in text_l:
            location_keywords.append(loc)
    
    if duration_filter or location_keywords:
        for tour_idx, tour_data in TOURS_DB.items():
            score = 0.0
            
            if duration_filter and 'duration' in tour_data:
                if duration_filter in tour_data['duration'].lower():
                    score += 1.0
            
            if location_keywords and 'location' in tour_data:
                location = tour_data['location'].lower()
                for lk in location_keywords:
                    if lk in location:
                        score += 1.0
                        break
            
            if score > 0:
                feature_matches.append((score, tour_idx))
        
        if feature_matches:
            feature_matches.sort(reverse=True)
            best_score = feature_matches[0][0]
            selected = [idx for score, idx in feature_matches if score == best_score]
            return selected
    
    # 3. Context reference
    context_refs = ['tour này', 'tour đó', 'tour đang nói', 'cái tour', 'này', 'đó', 'nó']
    if any(ref in text_l for ref in context_refs):
        if context.last_tour_indices:
            return context.last_tour_indices
    
    return []

def get_passages_by_field(field_name: str, limit: int = 50, tour_indices: Optional[List[int]] = None) -> List[Tuple[float, dict]]:
    exact_matches: List[Tuple[float, dict]] = []
    global_matches: List[Tuple[float, dict]] = []
    
    for m in MAPPING:
        path = m.get("path", "")
        if path.endswith(f".{field_name}") or f".{field_name}" in path:
            is_exact_match = False
            if tour_indices:
                for ti in tour_indices:
                    if f"[{ti}]" in path:
                        is_exact_match = True
                        break
            
            if is_exact_match:
                exact_matches.append((2.0, m))
            elif not tour_indices:
                global_matches.append((1.0, m))
    
    all_results = exact_matches + global_matches
    all_results.sort(key=lambda x: x[0], reverse=True)
    return all_results[:limit]

def handle_field_query(field_name: str, tour_indices: Optional[List[int]] = None, context=None) -> Tuple[str, List[dict]]:
    passages = []
    
    if not tour_indices:
        if field_name == "tour_name":
            tour_names = []
            for idx, tour_data in TOURS_DB.items():
                name = tour_data.get("tour_name")
                if name and name not in tour_names:
                    tour_names.append(name)
            
            if tour_names:
                answer = "Danh sách các tour hiện có:\n"
                for i, name in enumerate(tour_names[:10], 1):
                    answer += f"{i}. {name}\n"
                if len(tour_names) > 10:
                    answer += f"... và {len(tour_names) - 10} tour khác."
                return answer, []
            else:
                return "Hiện chưa có thông tin tour. Vui lòng liên hệ hotline 0332510486.", []
        
        all_values = []
        for idx, tour_data in TOURS_DB.items():
            if field_name in tour_data:
                value = tour_data[field_name]
                if isinstance(value, list):
                    all_values.extend(value)
                else:
                    all_values.append(value)
        
        if all_values:
            sample = all_values[:5]
            answer = f"Thông tin về {field_name} từ các tour:\n"
            for val in sample:
                answer += f"• {val}\n"
            if len(all_values) > 5:
                answer += f"... và {len(all_values) - 5} thông tin khác."
            return answer, []
        else:
            if field_name in FIELD_INFERENCE_RULES:
                rules = FIELD_INFERENCE_RULES[field_name]
                answer = f"Thông tin chung về {field_name}:\n"
                for rule_key, rule_value in rules.items():
                    answer += f"• {rule_value}\n"
                return answer, []
            else:
                return f"Hiện không có thông tin về {field_name} trong dữ liệu. Vui lòng liên hệ hotline 0332510486.", []
    
    answers = []
    inference_used = False
    
    for idx in tour_indices:
        if idx in TOURS_DB:
            tour_data = TOURS_DB[idx]
            
            if field_name in tour_data:
                field_value = tour_data[field_name]
                if isinstance(field_value, list):
                    answer = "\n".join([f"• {item}" for item in field_value])
                else:
                    answer = field_value
                
                tour_name = tour_data.get("tour_name", f"Tour #{idx}")
                answers.append(f"**{tour_name}**:\n{answer}")
                
                for m in MAPPING:
                    if f"[{idx}]" in m.get("path", "") and f".{field_name}" in m.get("path", ""):
                        passages.append(m)
            else:
                tour_name = tour_data.get("tour_name", f"Tour #{idx}")
                duration = tour_data.get("duration", "")
                inference_answer = None
                
                if field_name == "price" and FIELD_INFERENCE_RULES.get("price"):
                    if "1 ngày" in duration:
                        inference_answer = FIELD_INFERENCE_RULES["price"]["default_for_1day"]
                    elif "2 ngày" in duration:
                        inference_answer = FIELD_INFERENCE_RULES["price"]["default_for_2day"]
                
                elif field_name == "meals" and FIELD_INFERENCE_RULES.get("meals"):
                    if "1 ngày" in duration:
                        inference_answer = FIELD_INFERENCE_RULES["meals"]["1day_tours"]
                    elif "2 ngày" in duration:
                        inference_answer = FIELD_INFERENCE_RULES["meals"]["2day_tours"]
                
                elif field_name == "accommodation" and FIELD_INFERENCE_RULES.get("accommodation"):
                    if "1 ngày" in duration:
                        inference_answer = FIELD_INFERENCE_RULES["accommodation"]["1day_tours"]
                    elif "2 ngày" in duration:
                        inference_answer = FIELD_INFERENCE_RULES["accommodation"]["2day_tours"]
                
                if inference_answer:
                    answers.append(f"**{tour_name}**:\n{inference_answer} (thông tin ước tính)")
                    inference_used = True
    
    if answers:
        answer_text = "\n\n".join(answers)
        if inference_used:
            answer_text += "\n\n*Ghi chú: Thông tin dựa trên ước tính. Vui lòng liên hệ hotline 0332510486 để biết chính xác.*"
        return answer_text, passages
    
    if field_name == "price":
        return "Thông tin giá cả đang được cập nhật. Vui lòng liên hệ hotline 0332510486 để được báo giá chính xác.", []
    elif field_name == "meals":
        return "Thông tin về bữa ăn đang được cập nhật. Các tour thường bao gồm ít nhất 1 bữa chính mỗi ngày.", []
    else:
        return f"Thông tin về {field_name} đang được cập nhật. Vui lòng liên hệ hotline 0332510486 để biết thêm chi tiết.", []

# =========== EMBEDDINGS ===========
@lru_cache(maxsize=8192)
def embed_text(text: str) -> Tuple[List[float], int]:
    if not text:
        return [], 0
    short = text if len(text) <= 2000 else text[:2000]
    
    if client is not None:
        try:
            resp = client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=short
            )
            if resp.data and len(resp.data) > 0:
                emb = resp.data[0].embedding
                return emb, len(emb)
        except Exception:
            logger.exception("OpenAI embedding call failed")
    
    # Deterministic fallback
    try:
        h = abs(hash(short)) % (10 ** 12)
        fallback_dim = 1536
        vec = [(float((h >> (i % 32)) & 0xFF) + (i % 7)) / 255.0 for i in range(fallback_dim)]
        return vec, fallback_dim
    except Exception:
        logger.exception("Fallback embedding generation failed")
        return [], 0

# =========== INDEX MANAGEMENT ===========
class NumpyIndex:
    def __init__(self, mat: Optional[np.ndarray] = None):
        if mat is None or getattr(mat, "size", 0) == 0:
            self.mat = np.empty((0, 0), dtype="float32")
            self.dim = None
        else:
            self.mat = mat.astype("float32")
            self.dim = self.mat.shape[1]

    def add(self, mat: np.ndarray):
        if getattr(mat, "size", 0) == 0:
            return
        mat = mat.astype("float32")
        if getattr(self.mat, "size", 0) == 0:
            self.mat = mat.copy()
            self.dim = mat.shape[1]
        else:
            if mat.shape[1] != self.dim:
                raise ValueError("Dimension mismatch")
            self.mat = np.vstack([self.mat, mat])

    def search(self, qvec: np.ndarray, k: int):
        if self.mat is None or getattr(self.mat, "size", 0) == 0:
            return np.array([[]], dtype="float32"), np.array([[]], dtype="int64")
        q = qvec.astype("float32")
        q = q / (np.linalg.norm(q, axis=1, keepdims=True) + 1e-12)
        m = self.mat / (np.linalg.norm(self.mat, axis=1, keepdims=True) + 1e-12)
        sims = np.dot(q, m.T)
        idx = np.argsort(-sims, axis=1)[:, :k]
        scores = np.take_along_axis(sims, idx, axis=1)
        return scores.astype("float32"), idx.astype("int64")

    @property
    def ntotal(self):
        return 0 if getattr(self.mat, "size", 0) == 0 else self.mat.shape[0]

    def save(self, path):
        try:
            np.savez_compressed(path, mat=self.mat)
            logger.info(f"Saved numpy index to {path}")
        except Exception as e:
            logger.error(f"Failed to save numpy index: {e}")

    @classmethod
    def load(cls, path):
        try:
            arr = np.load(path)
            mat = arr["mat"]
            logger.info(f"Loaded numpy index from {path}")
            return cls(mat=mat)
        except Exception as e:
            logger.error(f"Failed to load numpy index: {e}")
            return cls(None)

def load_mapping_from_disk(path=FAISS_MAPPING_PATH):
    global MAPPING, FLAT_TEXTS
    try:
        with open(path, "r", encoding="utf-8") as f:
            MAPPING[:] = json.load(f)
        FLAT_TEXTS[:] = [m.get("text", "") for m in MAPPING]
        logger.info("Loaded mapping from %s (%d entries)", path, len(MAPPING))
        return True
    except Exception as e:
        logger.error(f"Failed to load mapping from disk: {e}")
        return False

def save_mapping_to_disk(path=FAISS_MAPPING_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(MAPPING, f, ensure_ascii=False, indent=2)
        logger.info("Saved mapping to %s", path)
    except Exception as e:
        logger.error(f"Failed to save mapping: {e}")

def _index_dim(idx) -> Optional[int]:
    try:
        d = getattr(idx, "d", None)
        if isinstance(d, int) and d > 0:
            return d
    except Exception:
        pass
    try:
        d = getattr(idx, "dim", None)
        if isinstance(d, int) and d > 0:
            return d
    except Exception:
        pass
    try:
        if HAS_FAISS and isinstance(idx, faiss.Index):
            return int(idx.d)
    except Exception:
        pass
    return None

def choose_embedding_model_for_dim(dim: int) -> str:
    if dim == 1536:
        return "text-embedding-3-small"
    if dim == 3072:
        return "text-embedding-3-large"
    return os.environ.get("EMBEDDING_MODEL", EMBEDDING_MODEL)

def build_index(force_rebuild: bool = False) -> bool:
    global INDEX, MAPPING, FLAT_TEXTS, EMBEDDING_MODEL
    with INDEX_LOCK:
        use_faiss = FAISS_ENABLED and HAS_FAISS

        if not force_rebuild:
            if FAISS_ENABLED and use_faiss and os.path.exists(FAISS_INDEX_PATH) and os.path.exists(FAISS_MAPPING_PATH):
                try:
                    idx = faiss.read_index(FAISS_INDEX_PATH)
                    if load_mapping_from_disk(FAISS_MAPPING_PATH):
                        FLAT_TEXTS[:] = [m.get("text", "") for m in MAPPING]
                    idx_dim = _index_dim(idx)
                    if idx_dim:
                        EMBEDDING_MODEL = choose_embedding_model_for_dim(idx_dim)
                    INDEX = idx
                    index_tour_names()
                    logger.info(f"FAISS index loaded from {FAISS_INDEX_PATH}")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to load FAISS index: {e}")
            
            if os.path.exists(FALLBACK_VECTORS_PATH) and os.path.exists(FAISS_MAPPING_PATH):
                try:
                    idx = NumpyIndex.load(FALLBACK_VECTORS_PATH)
                    if load_mapping_from_disk(FAISS_MAPPING_PATH):
                        FLAT_TEXTS[:] = [m.get("text", "") for m in MAPPING]
                    INDEX = idx
                    idx_dim = getattr(idx, "dim", None)
                    if idx_dim:
                        EMBEDDING_MODEL = choose_embedding_model_for_dim(int(idx_dim))
                    index_tour_names()
                    logger.info("Fallback index loaded from disk")
                    return True
                except Exception as e:
                    logger.error(f"Failed to load fallback vectors: {e}")

        if not FLAT_TEXTS:
            logger.warning("No flattened texts to index")
            INDEX = None
            return False

        logger.info("Building embeddings for %d passages", len(FLAT_TEXTS))
        vectors = []
        dims = None
        for text in FLAT_TEXTS:
            emb, d = embed_text(text)
            if not emb:
                continue
            if dims is None:
                dims = d
            vectors.append(np.array(emb, dtype="float32"))
        
        if not vectors or dims is None:
            logger.warning("No vectors produced")
            INDEX = None
            return False

        try:
            mat = np.vstack(vectors).astype("float32")
            row_norms = np.linalg.norm(mat, axis=1, keepdims=True)
            mat = mat / (row_norms + 1e-12)

            if use_faiss:
                index = faiss.IndexFlatIP(dims)
                index.add(mat)
                INDEX = index
                try:
                    faiss.write_index(INDEX, FAISS_INDEX_PATH)
                    save_mapping_to_disk()
                except Exception as e:
                    logger.error(f"Failed to persist FAISS index: {e}")
                index_tour_names()
                logger.info("FAISS index built (dims=%d, n=%d)", dims, index.ntotal)
                return True
            else:
                idx = NumpyIndex(mat)
                INDEX = idx
                try:
                    idx.save(FALLBACK_VECTORS_PATH)
                    save_mapping_to_disk()
                except Exception as e:
                    logger.error(f"Failed to persist fallback vectors: {e}")
                index_tour_names()
                logger.info("Numpy fallback index built (dims=%d, n=%d)", dims, idx.ntotal)
                return True
        except Exception as e:
            logger.error(f"Error while building index: {e}")
            INDEX = None
            return False

# =========== QUERY INDEX ===========
def query_index(query: str, top_k: int = TOP_K) -> List[Tuple[float, dict]]:
    global INDEX
    if not query:
        return []
    if INDEX is None:
        built = build_index(force_rebuild=False)
        if not built or INDEX is None:
            logger.warning("Index not available")
            return []
    
    emb, d = embed_text(query)
    if not emb:
        return []
    
    vec = np.array(emb, dtype="float32").reshape(1, -1)
    vec = vec / (np.linalg.norm(vec, axis=1, keepdims=True) + 1e-12)

    idx_dim = _index_dim(INDEX)
    if idx_dim and vec.shape[1] != idx_dim:
        logger.error("Query dim %s != index dim %s", vec.shape[1], idx_dim)
        desired_model = choose_embedding_model_for_dim(idx_dim)
        if OPENAI_API_KEY:
            global EMBEDDING_MODEL
            EMBEDDING_MODEL = desired_model
            logger.info("Setting EMBEDDING_MODEL=%s and rebuilding", EMBEDDING_MODEL)
            rebuilt = build_index(force_rebuild=True)
            if not rebuilt:
                return []
            emb2, d2 = embed_text(query)
            if not emb2:
                return []
            vec = np.array(emb2, dtype="float32").reshape(1, -1)
            vec = vec / (np.linalg.norm(vec, axis=1, keepdims=True) + 1e-12)
        else:
            return []
    
    try:
        D, I = INDEX.search(vec, top_k)
    except Exception as e:
        logger.error(f"Error executing index.search: {e}")
        return []

    results: List[Tuple[float, dict]] = []
    try:
        scores = D[0].tolist() if getattr(D, "shape", None) else []
        idxs = I[0].tolist() if getattr(I, "shape", None) else []
        for score, idx in zip(scores, idxs):
            if idx < 0 or idx >= len(MAPPING):
                continue
            results.append((float(score), MAPPING[idx]))
    except Exception as e:
        logger.error(f"Failed to parse search results: {e}")
    
    return results

# =========== PROMPT COMPOSITION ===========
def compose_enhanced_prompt(top_passages: List[Tuple[float, dict]], context, tour_indices: List[int], user_message: str) -> str:
    header = (
        "Bạn là trợ lý AI của Ruby Wings - chuyên tư vấn du lịch trải nghiệm.\n"
        "TRẢ LỜI THEO CÁC NGUYÊN TẮC:\n"
        "1. ƯU TIÊN CAO NHẤT: Luôn sử dụng thông tin từ dữ liệu nội bộ được cung cấp thông qua hệ thống.\n"
        "2. Nếu thiếu thông tin CHI TIẾT, hãy tổng hợp và trả lời dựa trên THÔNG TIN CHUNG có sẵn trong dữ liệu nội bộ.\n"
        "3. Đối với tour cụ thể: nếu tìm thấy bất kỳ dữ liệu nội bộ liên quan nào (dù là tóm tắt, giá, lịch trình, ghi chú), PHẢI tổng hợp và trình bày rõ ràng; chỉ trả lời đang nâng cấp hoặc chưa có thông tin khi hoàn toàn không tìm thấy dữ liệu phù hợp.\n"
        "4. TUYỆT ĐỐI KHÔNG nói rằng bạn không đọc được file, không truy cập dữ liệu, hoặc từ chối trả lời khi đã có dữ liệu liên quan.\n"
        "5. Luôn giữ thái độ nhiệt tình, hữu ích, trả lời trực tiếp vào nội dung người dùng hỏi.\n\n"
        "Bạn là trợ lý AI của Ruby Wings — chuyên tư vấn ngành du lịch trải nghiệm, retreat, "
        "thiền, khí công, hành trình chữa lành và các hành trình tham quan linh hoạt theo nhu cầu. "
        "Trả lời ngắn gọn, chính xác, rõ ràng, tử tế và bám sát dữ liệu Ruby Wings.\n\n"
    )
    
    context_info = ""
    if context.user_preferences:
        prefs = []
        if context.user_preferences.get("duration_pref"):
            prefs.append(f"Thích tour {context.user_preferences['duration_pref']}")
        if context.user_preferences.get("price_range"):
            prefs.append(f"Ngân sách {context.user_preferences['price_range']}")
        if context.user_preferences.get("interests"):
            prefs.append(f"Quan tâm: {', '.join(context.user_preferences['interests'])}")
        if prefs:
            context_info = "THÔNG TIN NGƯỜI DÙNG:\n" + "\n".join(f"- {p}" for p in prefs) + "\n\n"
    
    tour_info = ""
    if tour_indices and TOURS_DB:
        tour_info = "TOUR ĐANG ĐƯỢC THẢO LUẬN:\n"
        for idx in tour_indices[:2]:
            if idx in TOURS_DB:
                tour = TOURS_DB[idx]
                name = tour.get("tour_name", f"Tour #{idx}")
                duration = tour.get("duration", "Không rõ")
                location = tour.get("location", "Không rõ")
                tour_info += f"- {name} (Thời gian: {duration}, Địa điểm: {location})\n"
        tour_info += "\n"
    
    if not top_passages:
        data_section = "Không tìm thấy dữ liệu nội bộ phù hợp."
    else:
        data_section = "DỮ LIỆU NỘI BỘ:\n"
        for i, (score, m) in enumerate(top_passages, start=1):
            data_section += f"\n[{i}] (score={score:.3f})\n{m.get('text','')}\n"
    
    prompt = header + context_info + tour_info + data_section
    prompt += "\n---\nTUÂN THỦ: Chỉ dùng dữ liệu trên; không bịa đặt; văn phong lịch sự."
    
    return prompt

# =========== KNOWLEDGE LOADER ===========
def load_knowledge(path: str = KNOWLEDGE_PATH):
    global KNOW, FLAT_TEXTS, MAPPING
    try:
        with open(path, "r", encoding="utf-8") as f:
            KNOW = json.load(f)
        logger.info(f"Successfully loaded knowledge from {path}")
    except Exception as e:
        logger.error(f"Could not open {path}: {e}")
        KNOW = {}
    
    FLAT_TEXTS = []
    MAPPING = []

    def scan(obj, prefix="root"):
        if isinstance(obj, dict):
            for k, v in obj.items():
                scan(v, f"{prefix}.{k}")
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                scan(v, f"{prefix}[{i}]")
        elif isinstance(obj, str):
            t = obj.strip()
            if t:
                FLAT_TEXTS.append(t)
                MAPPING.append({"path": prefix, "text": t})
        else:
            try:
                s = str(obj).strip()
                if s:
                    FLAT_TEXTS.append(s)
                    MAPPING.append({"path": prefix, "text": s})
            except Exception:
                pass

    scan(KNOW)
    logger.info("Knowledge scanned: %d passages", len(FLAT_TEXTS))

# =========== GOOGLE SHEETS FUNCTIONS ===========
def get_gspread_client(force_refresh: bool = False):
    global _gsheet_client
    
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        logger.error("GOOGLE_SERVICE_ACCOUNT_JSON not set")
        return None
    
    with _gsheet_client_lock:
        if _gsheet_client is not None and not force_refresh:
            return _gsheet_client
        
        try:
            info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_info(info, scopes=scopes)
            _gsheet_client = gspread.authorize(creds)
            logger.info("Google Sheets client initialized")
            return _gsheet_client
            
        except GoogleAuthError as e:
            logger.error(f"Google authentication error: {e}")
            _gsheet_client = None
            return None
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets client: {e}")
            _gsheet_client = None
            return None

def save_lead_to_fallback_storage(lead_data: dict) -> bool:
    if not ENABLE_FALLBACK_STORAGE:
        return False
    
    try:
        lead_data["timestamp"] = datetime.utcnow().isoformat()
        lead_data["synced"] = False
        
        with _fallback_storage_lock:
            leads = []
            if os.path.exists(FALLBACK_STORAGE_PATH):
                try:
                    with open(FALLBACK_STORAGE_PATH, 'r', encoding='utf-8') as f:
                        leads = json.load(f)
                        if not isinstance(leads, list):
                            leads = []
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning(f"Failed to read fallback storage: {e}")
                    leads = []
            
            leads.append(lead_data)
            
            with open(FALLBACK_STORAGE_PATH, 'w', encoding='utf-8') as f:
                json.dump(leads, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Lead saved to fallback storage: {FALLBACK_STORAGE_PATH}")
            return True
            
    except Exception as e:
        logger.error(f"Failed to save lead to fallback storage: {e}")
        return False

# =========== META CAPI ===========
@app.before_request
def track_meta_pageview():
    if HAS_META_CAPI and ENABLE_META_CAPI_CALL:
        try:
            send_meta_pageview(request)
        except Exception as e:
            logger.error(f"Meta CAPI tracking failed: {e}")

# =========== ROUTES ===========
@app.route("/")
def home():
    try:
        return jsonify({
            "status": "ok",
            "knowledge_count": len(FLAT_TEXTS) if FLAT_TEXTS is not None else 0,
            "index_exists": INDEX is not None,
            "index_dim": _index_dim(INDEX) if INDEX is not None else None,
            "embedding_model": EMBEDDING_MODEL,
            "faiss_available": HAS_FAISS,
            "faiss_enabled": FAISS_ENABLED,
            "google_sheets_enabled": ENABLE_GOOGLE_SHEETS,
            "fallback_storage_enabled": ENABLE_FALLBACK_STORAGE,
            "service_status": "operational",
            "upgrades": {
                "mandatory_filter": UPGRADE_1_MANDATORY_FILTER,
                "deduplication": UPGRADE_2_DEDUPLICATION,
                "enhanced_fields": UPGRADE_3_ENHANCED_FIELDS,
                "question_pipeline": UPGRADE_4_QUESTION_PIPELINE,
                "fuzzy_matching": UPGRADE_6_FUZZY_MATCHING,
                "auto_validation": UPGRADE_9_AUTO_VALIDATION,
                "template_system": UPGRADE_10_TEMPLATE_SYSTEM
            }
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/reindex", methods=["POST"])
def reindex():
    secret = request.headers.get("X-RBW-ADMIN", "")
    if not secret and os.environ.get("RBW_ALLOW_REINDEX", "") != "1":
        return jsonify({"error": "reindex not allowed"}), 403
    load_knowledge()
    ok = build_index(force_rebuild=True)
    return jsonify({"ok": ok, "count": len(FLAT_TEXTS)})

@app.route("/chat", methods=["POST"])
def chat():
    """Chat endpoint với tất cả upgrades tích hợp"""
    try:
        data = request.get_json() or {}
        user_message = (data.get("message") or "").strip()
        
        if not user_message:
            return jsonify({"reply": "Bạn chưa nhập câu hỏi."})
        
        # =========== UPGRADE 6: FUZZY MATCHING ===========
        if UPGRADE_6_FUZZY_MATCHING:
            # Tìm tour bằng fuzzy matching trước
            fuzzy_matches = FuzzyMatcher.find_best_match(user_message, TOUR_NAME_TO_INDEX)
            if fuzzy_matches:
                logger.info(f"Fuzzy matches found: {fuzzy_matches}")
        
        # =========== CONTEXT ===========
        session_id = extract_session_id(data, request.remote_addr)
        context = get_session_context(session_id)
        
        # =========== UPGRADE 1: MANDATORY FILTER ===========
        mandatory_filters = {}
        if UPGRADE_1_MANDATORY_FILTER:
            mandatory_filters = MandatoryFilter.extract_filters(user_message)
            if mandatory_filters:
                logger.info(f"Mandatory filters: {mandatory_filters}")
        
        # =========== TOUR DETECTION ===========
        tour_indices = resolve_tour_reference(user_message, context)
        
        # =========== APPLY MANDATORY FILTERS ===========
        if UPGRADE_1_MANDATORY_FILTER and mandatory_filters and TOURS_DB:
            filtered_indices = MandatoryFilter.filter_tours(TOURS_DB, mandatory_filters)
            if filtered_indices:
                # Kết hợp với tour_indices đã tìm được
                if tour_indices:
                    tour_indices = [idx for idx in tour_indices if idx in filtered_indices]
                else:
                    tour_indices = filtered_indices
        
        # =========== UPGRADE 3: ENHANCED FIELD DETECTION ===========
        requested_field = None
        field_confidence = 0.0
        if UPGRADE_3_ENHANCED_FIELDS:
            requested_field, field_confidence = EnhancedFieldDetector.detect_field(user_message)
            if requested_field and field_confidence > 0.3:
                logger.info(f"Detected field: {requested_field} (confidence: {field_confidence})")
        
        # Fallback to old field detection
        if not requested_field:
            for k, v in KEYWORD_FIELD_MAP.items():
                for kw in v["keywords"]:
                    if kw in user_message.lower():
                        requested_field = v["field"]
                        break
                if requested_field:
                    break
        
        # =========== UPGRADE 4: QUESTION PIPELINE ===========
        question_type = "information"
        if UPGRADE_4_QUESTION_PIPELINE:
            question_type = QuestionPipeline.classify_question(user_message)
            logger.info(f"Question type: {question_type}")
        
        # =========== PROCESS BY QUESTION TYPE ===========
        if question_type == "comparison" and len(tour_indices) >= 2:
            # UPGRADE 4: Comparison processing
            comparison_result = QuestionPipeline.process_comparison(tour_indices, TOURS_DB)
            return jsonify({
                "reply": comparison_result,
                "sources": [],
                "context": {
                    "tour_indices": tour_indices,
                    "session_id": session_id,
                    "question_type": question_type
                }
            })
        
        elif question_type == "recommendation":
            # UPGRADE 4: Recommendation processing
            recommendation_result = QuestionPipeline.process_recommendation(context.user_preferences, TOURS_DB)
            return jsonify({
                "reply": recommendation_result,
                "sources": [],
                "context": {
                    "tour_indices": tour_indices[:3] if tour_indices else [],
                    "session_id": session_id,
                    "question_type": question_type
                }
            })
        
        elif question_type == "list" or requested_field == "tour_name":
            # List tours
            top_results = get_passages_by_field("tour_name", limit=20)
            
            # UPGRADE 2: DEDUPLICATION
            if UPGRADE_2_DEDUPLICATION:
                top_results = DeduplicationEngine.deduplicate_passages(top_results)
            
            # UPGRADE 10: TEMPLATE SYSTEM
            if UPGRADE_10_TEMPLATE_SYSTEM and top_results:
                tour_items = []
                for i, (_, passage) in enumerate(top_results[:10], 1):
                    tour_name = passage.get("text", "").strip()
                    if not tour_name:
                        continue
                    
                    # Find tour info
                    tour_idx = None
                    for idx, m in enumerate(MAPPING):
                        if m.get("text", "").strip() == tour_name and ".tour_name" in m.get("path", ""):
                            match = re.search(r'\[(\d+)\]', m.get("path", ""))
                            if match:
                                tour_idx = int(match.group(1))
                            break
                    
                    duration = ""
                    location = ""
                    price = ""
                    summary = ""
                    
                    if tour_idx is not None and tour_idx in TOURS_DB:
                        tour_data = TOURS_DB[tour_idx]
                        duration = tour_data.get("duration", "")
                        location = tour_data.get("location", "")
                        price = tour_data.get("price", "")
                        summary = tour_data.get("summary", "")[:100] + "..." if tour_data.get("summary", "") and len(tour_data.get("summary", "")) > 100 else tour_data.get("summary", "")
                    
                    duration_emoji = "⏱️"
                    if "1 ngày" in duration:
                        duration_emoji = "🌅"
                    elif "2 ngày" in duration:
                        duration_emoji = "🌄"
                    
                    tour_items.append(TemplateSystem.render("tour_list_item",
                        index=i,
                        tour_name=tour_name,
                        duration_emoji=duration_emoji,
                        duration=duration or "Đang cập nhật",
                        location=location or "Đang cập nhật",
                        price=price or "Đang cập nhật",
                        summary=summary or "Tour trải nghiệm đặc sắc"
                    ))
                
                if tour_items:
                    reply = TemplateSystem.render("tour_list",
                        tour_items="\n".join(tour_items)
                    )
                else:
                    reply = "Hiện chưa có thông tin tour trong hệ thống."
            else:
                # Old list format
                names = []
                for _, m in top_results:
                    name = m.get("text", "").strip()
                    if name and name not in names:
                        names.append(name)
                
                if names:
                    reply = "✨ **Danh sách tour Ruby Wings:** ✨\n\n"
                    for i, name in enumerate(names[:10], 1):
                        reply += f"{i}. {name}\n"
                    if len(names) > 10:
                        reply += f"... và {len(names) - 10} tour khác."
                else:
                    reply = "Hiện chưa có thông tin tour trong hệ thống."
            
            return jsonify({
                "reply": reply,
                "sources": [m for _, m in top_results],
                "context": {
                    "tour_indices": tour_indices,
                    "session_id": session_id,
                    "question_type": question_type
                }
            })
        
        elif requested_field:
            # Field query
            field_answer, field_sources = handle_field_query(requested_field, tour_indices, context)
            reply = field_answer
            
            # UPGRADE 9: AUTO VALIDATION
            if UPGRADE_9_AUTO_VALIDATION:
                reply = AutoValidator.validate_response(reply)
            
            return jsonify({
                "reply": reply,
                "sources": field_sources,
                "context": {
                    "tour_indices": tour_indices,
                    "session_id": session_id,
                    "requested_field": requested_field,
                    "question_type": question_type
                }
            })
        
        else:
            # Default: semantic search + LLM
            top_k = int(data.get("top_k", TOP_K))
            top_results = query_index(user_message, top_k)
            
            # UPGRADE 2: DEDUPLICATION
            if UPGRADE_2_DEDUPLICATION:
                top_results = DeduplicationEngine.deduplicate_passages(top_results)
            
            system_prompt = compose_enhanced_prompt(top_results, context, tour_indices, user_message)
            messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_message}]

            reply = ""
            
            if client is not None:
                try:
                    resp = client.chat.completions.create(
                        model=CHAT_MODEL,
                        messages=messages,
                        temperature=0.2,
                        max_tokens=int(data.get("max_tokens", 700)),
                        top_p=0.95
                    )
                    if resp.choices and len(resp.choices) > 0:
                        reply = resp.choices[0].message.content or ""
                except Exception as e:
                    logger.error(f"OpenAI chat failed: {e}")
            
            if not reply:
                if top_results:
                    snippets = "\n\n".join([f"• {m.get('text')}" for _, m in top_results[:5]])
                    reply = f"**Thông tin nội bộ liên quan:**\n\n{snippets}"
                else:
                    reply = "Xin lỗi — hiện không có dữ liệu nội bộ liên quan. Vui lòng liên hệ hotline 0332510486 để được tư vấn trực tiếp."
            
            # UPGRADE 9: AUTO VALIDATION
            if UPGRADE_9_AUTO_VALIDATION:
                reply = AutoValidator.validate_response(reply)
            
            # UPGRADE 10: TEMPLATE FOR TOUR DETAILS
            if UPGRADE_10_TEMPLATE_SYSTEM and tour_indices and len(tour_indices) == 1:
                tour_idx = tour_indices[0]
                if tour_idx in TOURS_DB:
                    tour_data = TOURS_DB[tour_idx]
                    # Check if reply is asking for tour details
                    if any(keyword in user_message.lower() for keyword in ["chi tiết", "thông tin", "giới thiệu", "nói về"]):
                        detailed_reply = TemplateSystem.render("tour_detail",
                            tour_name=tour_data.get("tour_name", f"Tour #{tour_idx}"),
                            duration=tour_data.get("duration", "Đang cập nhật"),
                            location=tour_data.get("location", "Đang cập nhật"),
                            price=tour_data.get("price", "Đang cập nhật"),
                            summary=tour_data.get("summary", "Tour trải nghiệm đặc sắc của Ruby Wings"),
                            accommodation=tour_data.get("accommodation", "Đang cập nhật"),
                            meals=tour_data.get("meals", "Đang cập nhật"),
                            transport=tour_data.get("transport", "Đang cập nhật")
                        )
                        # Combine with LLM reply
                        reply = detailed_reply + "\n\n" + reply
            
            # Update context
            if tour_indices:
                tour_name = None
                for idx in tour_indices:
                    for m in MAPPING:
                        if f"[{idx}]" in m.get("path", "") and ".tour_name" in m.get("path", ""):
                            tour_name = m.get("text", "")
                            break
                    if tour_name:
                        break
                
                update_tour_context(session_id, tour_indices, tour_name)
            
            # Update user preferences
            text_l = user_message.lower()
            if any(word in text_l for word in ["thiên nhiên", "rừng", "cây cối", "núi"]):
                if "nature" not in context.user_preferences["interests"]:
                    context.user_preferences["interests"].append("nature")
            if any(word in text_l for word in ["lịch sử", "tri ân", "chiến tranh", "di tích"]):
                if "history" not in context.user_preferences["interests"]:
                    context.user_preferences["interests"].append("history")
            if any(word in text_l for word in ["văn hóa", "cộng đồng", "dân tộc", "truyền thống"]):
                if "culture" not in context.user_preferences["interests"]:
                    context.user_preferences["interests"].append("culture")
            
            if "1 ngày" in text_l or "1ngày" in text_l:
                context.user_preferences["duration_pref"] = "1day"
            elif "2 ngày" in text_l or "2ngày" in text_l:
                context.user_preferences["duration_pref"] = "2day"
            
            context.conversation_history.append({
                "timestamp": datetime.utcnow().isoformat(),
                "user_message": user_message,
                "tour_indices": tour_indices,
                "question_type": question_type
            })
            
            if len(context.conversation_history) > 10:
                context.conversation_history = context.conversation_history[-10:]
            
            return jsonify({
                "reply": reply,
                "sources": [m for _, m in top_results],
                "context": {
                    "tour_indices": tour_indices,
                    "session_id": session_id,
                    "question_type": question_type,
                    "user_preferences": context.user_preferences
                }
            })
    
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}", exc_info=True)
        return jsonify({
            "reply": "Có lỗi xảy ra khi xử lý yêu cầu. Vui lòng thử lại sau.",
            "error": str(e)
        }), 500

@app.route('/api/save-lead', methods=['POST'])
def save_lead_to_sheet():
    """Save lead to Google Sheets với fallback"""
    try:
        if not request.is_json:
            return jsonify({
                "error": "Content-Type must be application/json",
                "success": False
            }), 400

        data = request.get_json() or {}
        phone = (data.get("phone") or "").strip()
        if not phone:
            return jsonify({
                "error": "Phone number is required",
                "success": False
            }), 400

        lead_data = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "source_channel": data.get("source_channel", "Website"),
            "action_type": data.get("action_type", "Click Call"),
            "page_url": data.get("page_url", ""),
            "contact_name": data.get("contact_name", ""),
            "phone": phone,
            "service_interest": data.get("service_interest", ""),
            "note": data.get("note", ""),
            "status": "New",
            "sync_method": "unknown"
        }
        
        logger.info(f"Processing lead: {phone}")

        sheets_success = False
        if ENABLE_GOOGLE_SHEETS:
            try:
                gc = get_gspread_client()
                if gc:
                    sh = gc.open_by_key(GOOGLE_SHEET_ID)
                    ws = sh.worksheet(GOOGLE_SHEET_NAME)
                    
                    row = [
                        lead_data["timestamp"],
                        lead_data["source_channel"],
                        lead_data["action_type"],
                        lead_data["page_url"],
                        lead_data["contact_name"],
                        lead_data["phone"],
                        lead_data["service_interest"],
                        lead_data["note"],
                        lead_data["status"]
                    ]
                    
                    ws.append_row(row, value_input_option="USER_ENTERED")
                    lead_data["sync_method"] = "google_sheets"
                    sheets_success = True
                    
                    logger.info(f"Lead saved to Google Sheets: {phone}")
                    
                    # Meta CAPI
                    if HAS_META_CAPI and ENABLE_META_CAPI_CALL:
                        try:
                            send_meta_lead(
                                request=request,
                                event_name="Lead",
                                phone=lead_data.get("phone"),
                                value=200000,
                                currency="VND",
                                content_name=lead_data.get("action_type", "Call / Consult")
                            )
                        except Exception as e:
                            logger.warning(f"Meta CAPI lead tracking failed: {e}")

            except Exception as e:
                logger.error(f"Google Sheets error: {e}")
                lead_data["error"] = str(e)

        fallback_success = False
        if ENABLE_FALLBACK_STORAGE:
            if not sheets_success:
                fallback_success = save_lead_to_fallback_storage(lead_data)
                if fallback_success:
                    lead_data["sync_method"] = "fallback_storage"
            else:
                save_lead_to_fallback_storage(lead_data)

        if sheets_success:
            return jsonify({
                "success": True,
                "message": "Lead saved successfully to Google Sheets",
                "data": {
                    "phone": phone,
                    "timestamp": lead_data["timestamp"],
                    "sync_method": "google_sheets"
                }
            }), 200
        elif fallback_success:
            return jsonify({
                "success": True,
                "message": "Lead saved to fallback storage",
                "warning": "Google Sheets synchronization failed",
                "data": {
                    "phone": phone,
                    "timestamp": lead_data["timestamp"],
                    "sync_method": "fallback_storage"
                }
            }), 200
        else:
            logger.error(f"Failed to save lead: {phone}")
            return jsonify({
                "success": False,
                "error": "Failed to save lead",
                "details": lead_data.get("error", "Unknown error")
            }), 500

    except Exception as e:
        logger.error(f"SAVE_LEAD_ERROR: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error",
            "details": "Please check server logs"
        }), 500

@app.route('/api/track-call', methods=['POST', 'OPTIONS'])
def track_call_event():
    """Track call button clicks"""
    try:
        if request.method == 'OPTIONS':
            response = jsonify({'status': 'ok'})
            response.headers.add('Access-Control-Allow-Origin', 'https://www.rubywings.vn')
            response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
            response.headers.add('Access-Control-Allow-Methods', 'POST, OPTIONS')
            return response
        
        data = request.get_json() or {}
        logger.info(f"Call button clicked: {data.get('phone', 'unknown')}")
        
        # Meta CAPI call
        if HAS_META_CAPI:
            try:
                from meta_capi import send_meta_call_button
                send_meta_call_button(
                    request=request,
                    page_url=data.get('page_url'),
                    phone=data.get('phone'),
                    call_type=data.get('call_type', 'regular')
                )
            except Exception as e:
                logger.warning(f"Meta CAPI call tracking failed: {e}")
        
        # Local log
        try:
            log_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "event": "call_button_click",
                "data": {
                    "phone": data.get('phone'),
                    "call_type": data.get('call_type'),
                    "page_url": data.get('page_url')
                }
            }
            
            logs_dir = "logs"
            if not os.path.exists(logs_dir):
                os.makedirs(logs_dir)
            
            log_file = os.path.join(logs_dir, f"call_clicks_{datetime.utcnow().strftime('%Y-%m-%d')}.json")
            
            logs = []
            if os.path.exists(log_file):
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        logs = json.load(f)
                except:
                    logs = []
            
            logs.append(log_entry)
            
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.warning(f"Failed to save call log: {e}")
        
        response = jsonify({
            "success": True,
            "message": "Call event tracked successfully",
            "meta_capi_sent": HAS_META_CAPI
        })
        response.headers.add('Access-Control-Allow-Origin', 'https://www.rubywings.vn')
        return response
    
    except Exception as e:
        logger.error(f"Track call error: {e}")
        response = jsonify({
            "success": False,
            "error": str(e)
        })
        response.status_code = 500
        response.headers.add('Access-Control-Allow-Origin', 'https://www.rubywings.vn')
        return response

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        sheets_status = "disabled"
        if ENABLE_GOOGLE_SHEETS:
            try:
                gc = get_gspread_client()
                if gc:
                    gc.open_by_key(GOOGLE_SHEET_ID)
                    sheets_status = "connected"
                else:
                    sheets_status = "client_error"
            except Exception as e:
                sheets_status = f"error: {type(e).__name__}"
        
        fallback_status = "disabled"
        if ENABLE_FALLBACK_STORAGE:
            try:
                if os.path.exists(FALLBACK_STORAGE_PATH):
                    with open(FALLBACK_STORAGE_PATH, 'r', encoding='utf-8') as f:
                        json.load(f)
                    fallback_status = "available"
                else:
                    fallback_status = "not_created"
            except Exception as e:
                fallback_status = f"error: {type(e).__name__}"
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "services": {
                "google_sheets": sheets_status,
                "fallback_storage": fallback_status,
                "openai": "available" if client else "unavailable",
                "faiss": "available" if HAS_FAISS else "unavailable",
                "index": "loaded" if (INDEX is not None) else "not_loaded"
            },
            "upgrades": {
                "mandatory_filter": UPGRADE_1_MANDATORY_FILTER,
                "deduplication": UPGRADE_2_DEDUPLICATION,
                "enhanced_fields": UPGRADE_3_ENHANCED_FIELDS,
                "question_pipeline": UPGRADE_4_QUESTION_PIPELINE,
                "fuzzy_matching": UPGRADE_6_FUZZY_MATCHING,
                "auto_validation": UPGRADE_9_AUTO_VALIDATION,
                "template_system": UPGRADE_10_TEMPLATE_SYSTEM
            },
            "counts": {
                "knowledge_passages": len(FLAT_TEXTS),
                "mapping_entries": len(MAPPING),
                "tour_names": len(TOUR_NAME_TO_INDEX),
                "tours_db": len(TOURS_DB)
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

# =========== INITIALIZATION ===========
def initialize_application():
    """Khởi tạo ứng dụng"""
    try:
        logger.info("Starting Ruby Wings Chatbot v3.0...")
        
        load_knowledge()
        
        if os.path.exists(FAISS_MAPPING_PATH):
            try:
                with open(FAISS_MAPPING_PATH, "r", encoding="utf-8") as f:
                    file_map = json.load(f)
                if file_map:
                    MAPPING[:] = file_map
                    FLAT_TEXTS[:] = [m.get("text", "") for m in MAPPING]
                    index_tour_names()
                    build_tours_db()
                    logger.info("Mapping loaded from disk")
                    logger.info(f"Built tours database: {len(TOURS_DB)} tours")
            except Exception as e:
                logger.warning(f"Could not load mapping from disk: {e}")
                index_tour_names()
                build_tours_db()
        else:
            index_tour_names()
            build_tours_db()
        
        # Initialize Google Sheets in background
        if ENABLE_GOOGLE_SHEETS and GOOGLE_SERVICE_ACCOUNT_JSON:
            def init_gsheets():
                try:
                    client = get_gspread_client()
                    if client:
                        logger.info("Google Sheets client initialized")
                except Exception as e:
                    logger.error(f"Google Sheets init failed: {e}")
            
            threading.Thread(target=init_gsheets, daemon=True).start()
        
        # Build index in background
        def build_index_background():
            try:
                built = build_index(force_rebuild=False)
                if built:
                    logger.info("Index built successfully")
                else:
                    logger.warning("Index building failed")
            except Exception as e:
                logger.error(f"Background index build failed: {e}")
        
        threading.Thread(target=build_index_background, daemon=True).start()
        
        logger.info("✅ Application initialization completed")
        
    except Exception as e:
        logger.error(f"Application initialization failed: {e}")
        raise

# =========== STARTUP ===========
if __name__ == "__main__":
    initialize_application()
    
    if MAPPING and not os.path.exists(FAISS_MAPPING_PATH):
        try:
            save_mapping_to_disk()
        except Exception as e:
            logger.error(f"Failed to save initial mapping: {e}")
    
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 10000))
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    
    logger.info(f"Starting Flask server on {host}:{port} (debug={debug})")
    logger.info("Active upgrades:")
    logger.info(f"  • Mandatory Filter: {UPGRADE_1_MANDATORY_FILTER}")
    logger.info(f"  • Deduplication: {UPGRADE_2_DEDUPLICATION}")
    logger.info(f"  • Enhanced Fields: {UPGRADE_3_ENHANCED_FIELDS}")
    logger.info(f"  • Question Pipeline: {UPGRADE_4_QUESTION_PIPELINE}")
    logger.info(f"  • Fuzzy Matching: {UPGRADE_6_FUZZY_MATCHING}")
    logger.info(f"  • Auto Validation: {UPGRADE_9_AUTO_VALIDATION}")
    logger.info(f"  • Template System: {UPGRADE_10_TEMPLATE_SYSTEM}")
    
    app.run(host=host, port=port, debug=debug)
else:
    initialize_application()