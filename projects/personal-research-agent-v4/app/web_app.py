import hashlib
import hmac
import json
import logging
import os
import uuid
from urllib.parse import parse_qsl
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from app import db, db_users
from app import config as app_config
from app.pipeline import SUPPORTED_LANGUAGES

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="PRA Telegram Web App Configuration API")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

def validate_telegram_init_data(init_data: str) -> dict | None:
    bot_token = os.getenv("TELEGRAM_TOKEN")
    if not bot_token or not init_data:
        return None
    
    parsed = dict(parse_qsl(init_data))
    if "hash" not in parsed:
        return None
        
    hash_val = parsed.pop("hash")
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    digest = hmac.new(secret, data_check_string.encode(), hashlib.sha256).hexdigest()
    
    if hmac.compare_digest(digest, hash_val):
        if "user" in parsed:
            try:
                return json.loads(parsed["user"])
            except json.JSONDecodeError:
                return None
    return None

def get_db_path():
    return app_config.load_app_config().runtime_db_path

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index_path):
        return "<h1>Static UI not found</h1>"
    with open(index_path, "r", encoding="utf-8") as f:
        return f.read()

# ======================= SCHEMAS =======================

class SubtopicModel(BaseModel):
    name: str
    weight: float = 0.5

class TopicModel(BaseModel):
    name: str
    geo_scope: Optional[str] = "world"
    subtopics: List[SubtopicModel] = []

class MemoryFactModel(BaseModel):
    id: Optional[int] = None
    key: str
    value: str

class ProfileUpdate(BaseModel):
    language: str
    home_location: str
    desired_depth: str
    delivery_email: str
    topics: List[TopicModel]
    facts: List[MemoryFactModel]
    deleted_fact_ids: List[int] = []

# ========================================================

@app.get("/api/profile")
async def get_profile(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("tma "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    user_data = validate_telegram_init_data(authorization[4:])
    if not user_data or "id" not in user_data:
        raise HTTPException(status_code=403, detail="Invalid Telegram Auth")
        
    chat_id = int(user_data["id"])
    db_path = get_db_path()
    
    user = db_users.ensure_user(chat_id=chat_id, db_path=db_path)
    user_id = int(user["id"])
    profile = db.get_profile(user_id=user_id, db_path=db_path) or {}
    explicit = dict(profile.get("explicit_preferences") or {})
    topic_settings = explicit.get("topic_settings", {})
    
    # 1. Fetch Topics & Subtopics
    raw_subtopics = db.list_topic_weights(user_id=user_id, db_path=db_path)
    base_topics = user.get("topics") or []
    
    topics_map = {}
    for t in base_topics:
        topics_map[t] = {
            "name": t,
            "geo_scope": topic_settings.get(t, {}).get("geo_scope", "world"),
            "subtopics": []
        }
        
    for row in raw_subtopics:
        t_name = row["topic"]
        if t_name in topics_map:
            topics_map[t_name]["subtopics"].append({
                "name": row["subtopic"],
                "weight": row["weight"]
            })
            
    # 2. Fetch Memories / Facts
    raw_facts = db.list_profile_facts(user_id=user_id, db_path=db_path)
    facts = []
    for f in raw_facts:
        facts.append({
            "id": f["id"],
            "key": f["fact_key"],
            "value": f["fact_value"].get("statement", str(f["fact_value"])) if isinstance(f["fact_value"], dict) else str(f["fact_value"])
        })
    
    return {
        "topics": list(topics_map.values()),
        "facts": facts,
        "language": profile.get("language") or user.get("language") or "en",
        "home_location": profile.get("home_location") or "",
        "desired_depth": profile.get("desired_depth") or "standard",
        "delivery_email": explicit.get("delivery_email", "")
    }

@app.post("/api/profile")
async def update_profile(data: ProfileUpdate, authorization: str = Header(None)):
    if not authorization or not authorization.startswith("tma "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    user_data = validate_telegram_init_data(authorization[4:])
    if not user_data or "id" not in user_data:
        raise HTTPException(status_code=403, detail="Invalid Telegram Auth")
        
    chat_id = int(user_data["id"])
    db_path = get_db_path()
    
    user = db_users.ensure_user(chat_id=chat_id, db_path=db_path)
    user_id = int(user["id"])
    profile = db.get_profile(user_id=user_id, db_path=db_path) or {}
    explicit = dict(profile.get("explicit_preferences") or {})
    topic_settings = explicit.get("topic_settings", {})
    
    # Process Topics
    topic_names = [t.name.strip() for t in data.topics if t.name.strip()]
    db_users.update_user_topics(chat_id=chat_id, topics=topic_names, db_path=db_path)
    explicit["topics"] = topic_names
    
    for t in data.topics:
        t_name = t.name.strip()
        if not t_name: continue
        # Geo_scope settings
        if t_name not in topic_settings:
            topic_settings[t_name] = {}
        topic_settings[t_name]["geo_scope"] = t.geo_scope
        
        # Subtopics graph
        if hasattr(db, "set_topic_weight"):
            for st in t.subtopics:
                db.set_topic_weight(user_id=user_id, topic=t_name, subtopic=st.name, weight=st.weight, db_path=db_path)
            
            # NOTE: We aren't doing strict deletion of unlisted subtopics to keep history, but we could disable them.
            
    explicit["topic_settings"] = topic_settings
    explicit["delivery_email"] = data.delivery_email.strip()
    
    # Process Facts/Memory
    for fact_id in data.deleted_fact_ids:
        db.delete_profile_fact(user_id=user_id, fact_id=fact_id, db_path=db_path)
        
    for f in data.facts:
        if not f.value.strip(): continue
        fact_key = f.key or f"user_memory_{str(uuid.uuid4())[:8]}"
        db.upsert_profile_fact(
            user_id=user_id, 
            fact_key=fact_key, 
            fact_value={"statement": f.value.strip()}, 
            source="dashboard", 
            db_path=db_path
        )
        
    # Language Context update
    lang = data.language.lower() if data.language.lower() in SUPPORTED_LANGUAGES else (profile.get("language") or "en")
    db_users.update_user_language(chat_id=chat_id, language=lang, db_path=db_path)
    
    depth = data.desired_depth if data.desired_depth in {"brief", "standard", "deep"} else profile.get("desired_depth")
    location = data.home_location.strip()
    
    db.upsert_profile(
        user_id=user_id,
        language=lang,
        home_location=location,
        desired_depth=depth,
        explicit_preferences=explicit,
        db_path=db_path
    )
    
    return {"status": "success", "message": "Profile updated successfully"}
