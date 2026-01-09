#!/usr/bin/env python3
"""
build_index.py - Tạo đầy đủ các file index cho Ruby Wings Chatbot
Phiên bản đơn giản, dễ hiểu, tương thích với app.py
"""

import os
import json
import numpy as np
import re
from typing import List, Dict, Any
from datetime import datetime
import unicodedata
import hashlib

# =========== CONFIGURATION ===========
# Đọc từ biến môi trường, nếu không có dùng giá trị mặc định
KNOWLEDGE_PATH = os.environ.get("KNOWLEDGE_PATH", "knowledge.json")
FAISS_INDEX_PATH = os.environ.get("FAISS_INDEX_PATH", "faiss_index.bin")
FAISS_MAPPING_PATH = os.environ.get("FAISS_MAPPING_PATH", "faiss_mapping.json")
FAISS_META_PATH = os.environ.get("FAISS_META_PATH", "faiss_index_meta.json")
FALLBACK_VECTORS_PATH = os.environ.get("FALLBACK_VECTORS_PATH", "vectors.npz")
OLD_FAISS_PATH = "index.faiss"  # Giữ cố định cho backward compatibility

# OpenAI config - đọc từ biến môi trường
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
USE_OPENAI = bool(OPENAI_API_KEY)

# Model config - đọc từ biến môi trường
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
CHAT_MODEL = os.environ.get("CHAT_MODEL", "gpt-4o-mini")

# Kiểm tra các biến môi trường quan trọng
print("🔍 KIỂM TRA BIẾN MÔI TRƯỜNG:")
print(f"   KNOWLEDGE_PATH: {KNOWLEDGE_PATH}")
print(f"   FAISS_INDEX_PATH: {FAISS_INDEX_PATH}")
print(f"   FAISS_MAPPING_PATH: {FAISS_MAPPING_PATH}")
print(f"   FALLBACK_VECTORS_PATH: {FALLBACK_VECTORS_PATH}")
print(f"   EMBEDDING_MODEL: {EMBEDDING_MODEL}")
print(f"   CHAT_MODEL: {CHAT_MODEL}")
print(f"   OPENAI_API_KEY có tồn tại: {bool(OPENAI_API_KEY)}")
print(f"   GOOGLE_SERVICE_ACCOUNT_JSON có tồn tại: {bool(os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON'))}")


# =========== FLATTEN KNOWLEDGE ===========
def load_and_flatten_knowledge(knowledge_path: str) -> tuple:
    """
    Đọc knowledge.json và flatten thành danh sách văn bản
    Trả về: (FLAT_TEXTS, MAPPING)
    """
    print(f"📖 Đang đọc {knowledge_path}...")
    
    try:
        with open(knowledge_path, "r", encoding="utf-8") as f:
            KNOW = json.load(f)
        print(f"✅ Đọc thành công: {len(KNOW)} keys trong knowledge")
    except Exception as e:
        print(f"❌ Lỗi đọc {knowledge_path}: {e}")
        return [], []
    
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
    print(f"✅ Flatten thành công: {len(FLAT_TEXTS)} passages")
    return FLAT_TEXTS, MAPPING

# =========== EMBEDDINGS ===========
def deterministic_embedding(text: str, dim: int = 1536) -> List[float]:
    """
    Tạo embedding deterministic (ổn định) cho text
    Dùng khi không có OpenAI API
    """
    if not text:
        return [0.0] * dim
    
    # Lấy 2000 ký tự đầu
    short = text if len(text) <= 2000 else text[:2000]
    
    # Tạo hash ổn định
    h = abs(hash(short)) % (10 ** 12)
    
    # Tạo vector dựa trên hash
    vec = []
    for i in range(dim):
        # Dùng hash và index để tạo giá trị pseudo-random
        val = ((h >> (i % 32)) & 0xFF) / 255.0
        # Thêm một chút variation dựa trên ký tự
        if i < len(short):
            val = (val + ord(short[i % len(short)]) / 255.0) / 2.0
        vec.append(val)
    
    # Normalize
    norm = sum(v*v for v in vec) ** 0.5
    if norm > 0:
        vec = [v/norm for v in vec]
    
    return vec

def embed_with_openai(text: str, client, model: str = "text-embedding-3-small") -> List[float]:
    """
    Tạo embedding bằng OpenAI API
    """
    if not text:
        return []
    
    short = text if len(text) <= 2000 else text[:2000]
    
    try:
        resp = client.embeddings.create(
            model=model,
            input=short
        )
        if resp.data and len(resp.data) > 0:
            return resp.data[0].embedding
    except Exception as e:
        print(f"⚠️ OpenAI embedding lỗi: {e}")
    
    return []

# =========== BUILD INDEXES ===========
def build_faiss_index(flat_texts: List[str], mapping: List[Dict]) -> bool:
    """
    Xây dựng FAISS index và lưu các file liên quan
    """
    print("\n🔧 Đang xây dựng FAISS index...")
    
    # Kiểm tra FAISS
    try:
        import faiss
        print("✅ FAISS library có sẵn")
    except ImportError:
        print("❌ FAISS không cài đặt. Chạy: pip install faiss-cpu")
        return False
    
    # Kiểm tra OpenAI
    client = None
    if USE_OPENAI:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            print("✅ OpenAI client khởi tạo thành công")
        except Exception as e:
            print(f"⚠️ OpenAI client lỗi: {e}")
            client = None
    else:
        print("ℹ️  Sử dụng deterministic embedding (không có OpenAI API)")
    
    # Tạo embeddings
    print(f"📊 Tạo embeddings cho {len(flat_texts)} passages...")
    
    vectors = []
    for i, text in enumerate(flat_texts):
        if i % 50 == 0:
            print(f"  Đang xử lý {i}/{len(flat_texts)}...")
        
        if client:
            emb = embed_with_openai(text, client, EMBEDDING_MODEL)
        else:
            emb = deterministic_embedding(text, 1536)
        
        if emb:
            vectors.append(np.array(emb, dtype="float32"))
        else:
            # Fallback deterministic
            vectors.append(np.array(deterministic_embedding(text, 1536), dtype="float32"))
    
    if not vectors:
        print("❌ Không tạo được vectors nào")
        return False
    
    # Tạo matrix
    mat = np.vstack(vectors).astype("float32")
    print(f"✅ Tạo matrix: {mat.shape[0]} vectors, {mat.shape[1]} dimensions")
    
    # Normalize cho cosine similarity
    row_norms = np.linalg.norm(mat, axis=1, keepdims=True)
    mat = mat / (row_norms + 1e-12)
    
    # Tạo FAISS index
    dim = mat.shape[1]
    index = faiss.IndexFlatIP(dim)  # Inner Product = cosine similarity
    index.add(mat)
    
    # =========== LƯU CÁC FILE ===========
    
    # 1. Lưu FAISS index chính
    print(f"\n💾 Đang lưu FAISS index vào {FAISS_INDEX_PATH}...")
    faiss.write_index(index, FAISS_INDEX_PATH)
    print(f"✅ Đã lưu: {FAISS_INDEX_PATH}")
    
    # 2. Lưu mapping
    print(f"💾 Đang lưu mapping vào {FAISS_MAPPING_PATH}...")
    with open(FAISS_MAPPING_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã lưu: {FAISS_MAPPING_PATH}")
    
    # 3. Lưu metadata
    print(f"💾 Đang lưu metadata vào {FAISS_META_PATH}...")
    meta_data = {
        "embedding_model": EMBEDDING_MODEL,
        "dimension": int(dim),
        "total_vectors": len(flat_texts),
        "created_at": datetime.now().isoformat(),
        "index_type": "IndexFlatIP",
        "normalized": True,
        "similarity_metric": "cosine",
        "has_openai": USE_OPENAI,
        "version": "2.1.1"
    }
    with open(FAISS_META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta_data, f, indent=2)
    print(f"✅ Đã lưu: {FAISS_META_PATH}")
    
    # 4. Lưu file FAISS cũ (backward compatibility)
    print(f"💾 Đang lưu backup index vào {OLD_FAISS_PATH}...")
    faiss.write_index(index, OLD_FAISS_PATH)
    print(f"✅ Đã lưu: {OLD_FAISS_PATH}")
    
    # 5. Lưu numpy vectors (fallback)
    print(f"💾 Đang lưu numpy vectors vào {FALLBACK_VECTORS_PATH}...")
    np.savez_compressed(FALLBACK_VECTORS_PATH, mat=mat)
    print(f"✅ Đã lưu: {FALLBACK_VECTORS_PATH}")
    
    # Thông tin tổng kết
    print("\n" + "="*60)
    print("🎉 HOÀN THÀNH XÂY DỰNG INDEX!")
    print("="*60)
    print(f"📊 Tổng số passages: {len(flat_texts)}")
    print(f"📐 Dimension: {dim}")
    print(f"🔢 Index size: {index.ntotal} vectors")
    print(f"🔧 Embedding method: {'OpenAI' if USE_OPENAI else 'Deterministic'}")
    print(f"💾 Các file đã tạo:")
    print(f"  1. {FAISS_INDEX_PATH} (FAISS index chính)")
    print(f"  2. {FAISS_MAPPING_PATH} (mapping văn bản)")
    print(f"  3. {FAISS_META_PATH} (metadata)")
    print(f"  4. {OLD_FAISS_PATH} (FAISS index cũ - compatibility)")
    print(f"  5. {FALLBACK_VECTORS_PATH} (numpy fallback)")
    print("="*60)
    
    return True

# =========== MAIN ===========
def main():
    print("="*60)
    print("🚀 RUBY WINGS - BUILD INDEX UTILITY")
    print("="*60)
    
    # Kiểm tra knowledge.json
    if not os.path.exists(KNOWLEDGE_PATH):
        print(f"❌ Không tìm thấy {KNOWLEDGE_PATH}")
        print(f"   Vui lòng đảm bảo file knowledge.json tồn tại trong thư mục này")
        return
    
    # 1. Đọc và flatten knowledge
    flat_texts, mapping = load_and_flatten_knowledge(KNOWLEDGE_PATH)
    if not flat_texts:
        print("❌ Không có dữ liệu để xây dựng index")
        return
    
    # 2. Xây dựng FAISS index
    success = build_faiss_index(flat_texts, mapping)
    
    if success:
        print("\n✨ TẤT CẢ HOÀN THÀNH! Bạn có thể chạy app.py bình thường.")
        print("👉 Chạy: python app.py")
    else:
        print("\n❌ Có lỗi xảy ra khi xây dựng index")

if __name__ == "__main__":
    main()