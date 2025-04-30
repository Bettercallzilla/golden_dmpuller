
import os
import json
import uuid
import time
import tempfile
import requests
import threading
from typing import List
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from instagrapi import Client

# Configuration
PORT = int(os.getenv("PORT", 3016))
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 5))  # seconds

if not WEBHOOK_URL:
    raise RuntimeError("WEBHOOK_URL environment variable is required")

# Directories and files
SESSIONS_DIR = "/data/sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)
LASTSEEN_FILE = os.path.join(SESSIONS_DIR, "last_seen.json")
RULES_FILE = os.path.join(SESSIONS_DIR, "rules.json")
ANALYTICS_FILE = os.path.join(SESSIONS_DIR, "analytics.json")

# Load or initialize persistent stores
last_seen = json.load(open(LASTSEEN_FILE)) if os.path.exists(LASTSEEN_FILE) else {}
rules = json.load(open(RULES_FILE)) if os.path.exists(RULES_FILE) else []
analytics = json.load(open(ANALYTICS_FILE)) if os.path.exists(ANALYTICS_FILE) else {}

def save_last_seen():
    with open(LASTSEEN_FILE, "w") as f:
        json.dump(last_seen, f)

def save_rules():
    with open(RULES_FILE, "w") as f:
        json.dump(rules, f)

def save_analytics():
    with open(ANALYTICS_FILE, "w") as f:
        json.dump(analytics, f)

def load_client(session_id: str) -> Client:
    path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
    if not os.path.exists(path):
        raise RuntimeError(f"Session {session_id} not found")
    settings = json.load(open(path))
    client = Client()
    client.set_settings(settings)
    return client

# Polling loop for incoming DMs
def poll_loop():
    while True:
        session_files = [f for f in os.listdir(SESSIONS_DIR) if f.endswith(".json") and f not in ["last_seen.json", "rules.json", "analytics.json"]]
        for filename in session_files:
            session_id = filename[:-5]
            try:
                client = load_client(session_id)
                threads = client.direct_threads()
                for thread in threads:
                    tid = str(thread.id)
                    msgs = list(reversed(thread.messages))
                    if tid not in last_seen and msgs:
                        last_seen[tid] = msgs[-1].id
                        save_last_seen()
                        continue
                    new_msgs = []
                    for m in msgs:
                        if m.id == last_seen.get(tid):
                            new_msgs = []
                            continue
                        new_msgs.append(m)
                    if new_msgs:
                        analytics.setdefault(session_id, {"received":0,"forwarded":0,"auto_replies":0,"sent":0})
                        analytics[session_id]["received"] += len(new_msgs)
                        payload = {
                            "session_id": session_id,
                            "thread_id": tid,
                            "messages": [{"id": m.id, "text": m.text, "from": m.user.username} for m in new_msgs]
                        }
                        try:
                            resp = requests.post(WEBHOOK_URL, json=payload, timeout=5)
                            resp.raise_for_status()
                            analytics[session_id]["forwarded"] += len(new_msgs)
                        except:
                            pass
                        # auto-reply
                        for m in new_msgs:
                            for rule in rules:
                                if rule["pattern"] in m.text:
                                    try:
                                        client.direct_send(rule["response"], [m.user.pk])
                                        analytics[session_id]["auto_replies"] += 1
                                    except:
                                        pass
                        last_seen[tid] = new_msgs[-1].id
                        save_last_seen()
                        save_analytics()
                json.dump(client.get_settings(), open(os.path.join(SESSIONS_DIR, filename), "w"))
            except:
                continue
        time.sleep(POLL_INTERVAL)

threading.Thread(target=poll_loop, daemon=True).start()

app = FastAPI(title="Instagram Full API + Dashboard + Auto-Reply", version="2.0.0")

class LoginRequest(BaseModel):
    username: str
    password: str

class SendRequest(BaseModel):
    session_id: str
    recipients: List[str]
    message: str
    interval: float = 0.0

class RuleRequest(BaseModel):
    pattern: str
    response: str

class PhotoRequest(BaseModel):
    session_id: str
    image_url: str
    caption: str = ""

class StoryRequest(BaseModel):
    session_id: str
    file_url: str
    is_video: bool = False

class LikeRequest(BaseModel):
    session_id: str
    media_id: str

class CommentRequest(BaseModel):
    session_id: str
    media_id: str
    text: str

class DeleteCommentRequest(BaseModel):
    session_id: str
    media_id: str
    comment_id: str

class FollowRequest(BaseModel):
    session_id: str
    username: str

class ReelRequest(BaseModel):
    session_id: str
    file_url: str
    caption: str = ""

@app.post("/login")
async def login(req: LoginRequest):
    client = Client()
    try:
        client.login(req.username, req.password)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Login failed: {e}")
    session_id = str(uuid.uuid4())
    json.dump(client.get_settings(), open(os.path.join(SESSIONS_DIR, f"{session_id}.json"), "w"))
    return {"session_id": session_id}

@app.get("/sessions")
async def list_sessions():
    return {"sessions": [f[:-5] for f in os.listdir(SESSIONS_DIR) if f.endswith(".json") and f not in ["last_seen.json", "rules.json", "analytics.json"]]}

@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
    if os.path.exists(path):
        os.remove(path)
        analytics.pop(session_id, None)
        save_analytics()
        return {"deleted": session_id}
    raise HTTPException(status_code=404, detail="Session not found")

@app.get("/dashboard")
async def dashboard():
    return {"dashboard": [
        {"session_id": sid, **analytics.get(sid, {"received":0,"forwarded":0,"auto_replies":0,"sent":0})}
        for sid in [f[:-5] for f in os.listdir(SESSIONS_DIR) if f.endswith(".json") and f not in ["last_seen.json","rules.json","analytics.json"]]
    ]}

@app.get("/analytics")
async def get_all_analytics():
    return analytics

@app.get("/analytics/{session_id}")
async def get_session_analytics(session_id: str):
    return analytics.get(session_id, {"received":0,"forwarded":0,"auto_replies":0,"sent":0})

@app.post("/rules")
async def add_rule(req: RuleRequest):
    r_id = str(uuid.uuid4())
    rules.append({"id": r_id, "pattern": req.pattern, "response": req.response})
    save_rules()
    return {"rule_id": r_id}

@app.get("/rules")
async def list_rules():
    return {"rules": rules}

@app.delete("/rules/{rule_id}")
async def delete_rule(rule_id: str):
    for r in rules:
        if r["id"] == rule_id:
            rules.remove(r)
            save_rules()
            return {"deleted": rule_id}
    raise HTTPException(status_code=404, detail="Rule not found")

@app.post("/send")
async def send_dm(req: SendRequest):
    client = load_client(req.session_id)
    results = {}
    for user in req.recipients:
        try:
            uid = client.user_id_from_username(user)
            client.direct_send(req.message, [uid])
            analytics.setdefault(req.session_id, {"received":0,"forwarded":0,"auto_replies":0,"sent":0})
            analytics[req.session_id]["sent"] += 1
            save_analytics()
            results[user] = "sent"
        except Exception as e:
            results[user] = f"error: {e}"
        if req.interval > 0:
            time.sleep(req.interval)
    return {"results": results}

@app.post("/upload_photo")
async def upload_photo(req: PhotoRequest):
    client = load_client(req.session_id)
    r = requests.get(req.image_url, stream=True)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail="Download failed")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
    for chunk in r.iter_content(1024):
        tmp.write(chunk)
    tmp.close()
    media = client.photo_upload(tmp.name, caption=req.caption)
    os.unlink(tmp.name)
    return {"media_id": media.pk}

@app.post("/upload_story")
async def upload_story(req: StoryRequest):
    client = load_client(req.session_id)
    r = requests.get(req.file_url, stream=True)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail="Download failed")
    suffix = ".mp4" if req.is_video else ".jpg"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    for chunk in r.iter_content(1024):
        tmp.write(chunk)
    tmp.close()
    if req.is_video:
        story = client.story_upload_video(tmp.name)
    else:
        story = client.story_upload_photo(tmp.name)
    os.unlink(tmp.name)
    return {"story_id": story.pk}

@app.post("/upload_reel")
async def upload_reel(req: ReelRequest):
    client = load_client(req.session_id)
    r = requests.get(req.file_url, stream=True)
    if r.status_code != 200:
        raise HTTPException(status_code=400, detail="Download failed")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    for chunk in r.iter_content(1024):
        tmp.write(chunk)
    tmp.close()
    clip = client.clip_upload(tmp.name, caption=req.caption)
    os.unlink(tmp.name)
    return {"reel_id": clip.pk}

@app.get("/feed")
async def get_feed(session_id: str = Query(...)):
    client = load_client(session_id)
    feed = client.feed_timeline()
    return {"feed": [m.dict() for m in feed]}

@app.get("/user/{username}")
async def get_user(username: str, session_id: str = Query(...)):
    client = load_client(session_id)
    uid = client.user_id_from_username(username)
    info = client.user_info(uid)
    return info.dict()

@app.get("/user/{username}/posts")
async def get_user_posts(username: str, session_id: str = Query(...), amount: int = Query(10)):
    client = load_client(session_id)
    uid = client.user_id_from_username(username)
    medias = client.user_medias(uid, amount)
    return {"posts": [m.dict() for m in medias]}

@app.post("/like")
async def like_media(req: LikeRequest):
    client = load_client(req.session_id)
    client.media_like(req.media_id)
    return {"liked": req.media_id}

@app.post("/unlike")
async def unlike_media(req: LikeRequest):
    client = load_client(req.session_id)
    client.media_unlike(req.media_id)
    return {"unliked": req.media_id}

@app.post("/comment")
async def comment_media(req: CommentRequest):
    client = load_client(req.session_id)
    comment_obj = client.media_comment(req.media_id, req.text)
    return {"comment_id": comment_obj.pk}

@app.post("/delete_comment")
async def delete_comment(req: DeleteCommentRequest):
    client = load_client(req.session_id)
    client.comment_delete(req.media_id, req.comment_id)
    return {"deleted_comment": req.comment_id}

@app.post("/follow")
async def follow_user(req: FollowRequest):
    client = load_client(req.session_id)
    target = client.user_id_from_username(req.username)
    client.user_follow(target)
    return {"followed": req.username}

@app.post("/unfollow")
async def unfollow_user(req: FollowRequest):
    client = load_client(req.session_id)
    target = client.user_id_from_username(req.username)
    client.user_unfollow(target)
    return {"unfollowed": req.username}

@app.get("/stories/{username}")
async def get_stories(username: str, session_id: str = Query(...)):
    client = load_client(session_id)
    uid = client.user_id_from_username(username)
    stories = client.user_stories(uid)
    return {"stories": [s.dict() for s in stories]}

@app.get("/search/users")
async def search_users(q: str, session_id: str = Query(...), amount: int = Query(10)):
    client = load_client(session_id)
    users = client.search_users(q)
    return {"users": [u.dict() for u in users[:amount]]}

@app.get("/search/hashtags/top")
async def search_hashtags(hashtag: str, session_id: str = Query(...), amount: int = Query(5)):
    client = load_client(session_id)
    top = client.hashtag_medias_top(hashtag, amount)
    return {"top_posts": [m.dict() for m in top]}

@app.get("/threads")
async def list_threads(session_id: str = Query(...)):
    client = load_client(session_id)
    threads = client.direct_threads()
    return {"threads": [t.dict() for t in threads]}

@app.get("/threads/{thread_id}/messages")
async def get_thread_messages(thread_id: str, session_id: str = Query(...)):
    client = load_client(session_id)
    thread = client.direct_thread(thread_id)
    return {"messages": [msg.dict() for msg in thread.messages]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, workers=4)
