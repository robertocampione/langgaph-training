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
from app import pipeline  # noqa: E402
from app.pipeline import SUPPORTED_LANGUAGES, normalize_topic_text

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
        t_normalized = normalize_topic_text(t)
        setting = topic_settings.get(t_normalized, {})
        geo_scope_val = setting.get("geo_scope", "world")
        locales_val = setting.get("locales", [])
        
        if locales_val and isinstance(locales_val, list) and len(locales_val) > 0:
            display_scope = locales_val[0]
        else:
            display_scope = geo_scope_val
            
        topics_map[t] = {
            "name": t,
            "geo_scope": display_scope,
            "subtopics": []
        }
        
    normalized_to_ui = {normalize_topic_text(t): t for t in base_topics}
        
    for row in raw_subtopics:
        db_topic = row["topic"]
        norm_db_topic = normalize_topic_text(db_topic)
        ui_name = normalized_to_ui.get(norm_db_topic)
        
        if ui_name and ui_name in topics_map:
            topics_map[ui_name]["subtopics"].append({
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
        t_normalized = normalize_topic_text(t_name)
        
        # Geo_scope vs Locales parsing
        input_scope = str(t.geo_scope).strip().lower()
        if input_scope in ("local", "world", "global", "auto", ""):
            geo_scope = input_scope or "world"
            locales_to_add = None
        else:
            # User typed an explicit location, map to local and set the locale
            geo_scope = "local"
            locales_to_add = [str(t.geo_scope).strip()]

        # Geo_scope settings
        if t_normalized not in topic_settings:
            topic_settings[t_normalized] = {}
        topic_settings[t_normalized]["geo_scope"] = geo_scope
        if locales_to_add:
            topic_settings[t_normalized]["locales"] = locales_to_add
            
        # VERY IMPORTANT: Update the explicit preferences list of subtopics so the pipeline doesn't respawn deleted ones
        topic_settings[t_normalized]["subtopics"] = [st.name for st in t.subtopics]
        
        # Subtopics graph
        if hasattr(db, "set_topic_weight"):
            for st in t.subtopics:
                db.set_topic_weight(user_id=user_id, topic=t_normalized, subtopic=st.name, weight=st.weight, db_path=db_path)
            
            # True deletion of subtopics to allow UI removal
            subtopics_to_keep = [st.name for st in t.subtopics]
            if len(subtopics_to_keep) > 0:
                sqlite_placeholders = ",".join(["?"] * len(subtopics_to_keep))
                pg_placeholders = ",".join(["%s"] * len(subtopics_to_keep))
                db._execute(
                    f"DELETE FROM user_topic_graph WHERE user_id = ? AND topic = ? AND subtopic NOT IN ({sqlite_placeholders})",
                    f"DELETE FROM user_topic_graph WHERE user_id = %s AND topic = %s AND subtopic NOT IN ({pg_placeholders})",
                    (user_id, t_normalized, *subtopics_to_keep),
                    db_path=db_path
                )
            else:
                db._execute(
                    "DELETE FROM user_topic_graph WHERE user_id = ? AND topic = ?",
                    "DELETE FROM user_topic_graph WHERE user_id = %s AND topic = %s",
                    (user_id, t_normalized),
                    db_path=db_path
                )
            
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
