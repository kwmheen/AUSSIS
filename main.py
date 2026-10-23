import json
import re
import socket
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response
import uvicorn

app = FastAPI()
ROOT = Path("data/projects")
ROOT.mkdir(parents=True, exist_ok=True)
SAFE_ID = re.compile(r"^[A-Za-z0-9_-]+$")


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass


manager = ConnectionManager()

app_state = {
    "project_id": None,
    "participant_id": None,
    "document_id": None,
    "document_name": "",
    "mode": None,
    "fields": [],
    "participant": {"name": "", "date": ""},
    "regions": {},
}


def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def new_id(prefix):
    return f"{prefix}_{uuid4().hex[:8]}"


def require_id(value: str):
    if not value or not SAFE_ID.match(value):
        raise HTTPException(status_code=400, detail="잘못된 ID입니다.")
    return value


def project_file(project_id: str) -> Path:
    return ROOT / f"{require_id(project_id)}.json"


def pdf_file(project_id: str, document_id: str) -> Path:
    folder = ROOT / require_id(project_id)
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{require_id(document_id)}.pdf"


def load_project(project_id: str) -> dict:
    path = project_file(project_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="프로젝트를 찾을 수 없습니다.")
    return json.loads(path.read_text(encoding="utf-8"))


def save_project(project: dict):
    path = project_file(project["id"])
    path.write_text(json.dumps(project, ensure_ascii=False, indent=2), encoding="utf-8")


def find_item(items, item_id):
    for item in items:
        if item.get("id") == item_id:
            return item
    return None


def public_project(project: dict) -> dict:
    copied = json.loads(json.dumps(project))
    for doc in copied.get("documents", []):
        doc.pop("pdfBase64", None)
    return copied


def project_summary(project: dict) -> dict:
    return {
        "id": project["id"],
        "name": project["name"],
        "createdAt": project.get("createdAt"),
        "documentCount": len(project.get("documents", [])),
        "participantCount": len(project.get("participants", [])),
    }


def geometry_fields(fields):
    cleaned = []
    for field in fields or []:
        cleaned.append({
            "id": field.get("id"),
            "page": int(field.get("page") or 1),
            "x": float(field.get("x") or 0),
            "y": float(field.get("y") or 0),
            "width": float(field.get("width") or 10),
            "height": float(field.get("height") or 8),
            "label": field.get("label") or "참여자 서명",
        })
    return cleaned


def session_fields(project, participant_id, document_id):
    document = find_item(project.get("documents", []), document_id)
    participant = find_item(project.get("participants", []), participant_id) if participant_id else None
    if not document:
        return []
    saved = {}
    if participant:
        saved = (participant.get("signatures") or {}).get(document_id) or {}
    merged = []
    for field in document.get("fields", []):
        item = {**field, "signed": False, "img": None}
        signature = saved.get(field["id"]) if isinstance(saved, dict) else None
        if signature and signature.get("signed"):
            item["signed"] = True
            item["img"] = signature.get("img")
        merged.append(item)
    return merged


def get_local_ipv4():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip and not ip.startswith("127."):
                return ip
    except Exception:
        pass
    return "127.0.0.1"


def persist_signature(field_id, image_data):
    project_id = app_state.get("project_id")
    participant_id = app_state.get("participant_id")
    document_id = app_state.get("document_id")
    if not project_id or not participant_id or not document_id or app_state.get("mode") != "live":
        return
    project = load_project(project_id)
    participant = find_item(project.get("participants", []), participant_id)
    document = find_item(project.get("documents", []), document_id)
    if not participant or not document:
        return
    if not any(field.get("id") == field_id for field in document.get("fields", [])):
        return
    participant.setdefault("signatures", {})
    participant["signatures"].setdefault(document_id, {})
    participant["signatures"][document_id][field_id] = {"signed": True, "img": image_data}
    save_project(project)


@app.get("/api/host")
def host_info(request: Request):
    ip = get_local_ipv4()
    port = request.url.port or 8000
    return {"ip": ip, "port": port, "padUrl": f"http://{ip}:{port}/pad"}


@app.get("/api/projects")
def list_projects():
    projects = []
    for path in sorted(ROOT.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            projects.append(project_summary(json.loads(path.read_text(encoding="utf-8"))))
        except Exception:
            continue
    return projects


@app.post("/api/projects")
async def create_project(payload: dict):
    name = str(payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="프로젝트 이름이 필요합니다.")
    project = {
        "id": new_id("proj"),
        "name": name,
        "createdAt": now_iso(),
        "documents": [],
        "participants": [],
    }
    save_project(project)
    return project


@app.get("/api/projects/{project_id}")
def get_project(project_id: str):
    return public_project(load_project(project_id))


@app.delete("/api/projects/{project_id}")
def delete_project(project_id: str):
    project = load_project(project_id)
    folder = ROOT / project["id"]
    if folder.exists():
        for child in folder.glob("*"):
            child.unlink(missing_ok=True)
        folder.rmdir()
    project_file(project_id).unlink(missing_ok=True)
    return {"ok": True}


@app.post("/api/projects/{project_id}/documents")
async def add_document(project_id: str, request: Request):
    project = load_project(project_id)
    data = await request.body()
    if not data.startswith(b"%PDF"):
        raise HTTPException(status_code=400, detail="PDF 파일이 아닙니다.")
    name = request.query_params.get("name") or "consent.pdf"
    document_id = new_id("doc")
    pdf_file(project_id, document_id).write_bytes(data)
    project["documents"].append({
        "id": document_id,
        "name": Path(name).name,
        "file": f"{document_id}.pdf",
        "fields": [],
    })
    save_project(project)
    return public_project(project)


@app.get("/api/projects/{project_id}/documents/{document_id}/file")
def get_document_file(project_id: str, document_id: str):
    project = load_project(project_id)
    if not find_item(project.get("documents", []), document_id):
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")
    path = pdf_file(project_id, document_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="PDF 파일이 없습니다.")
    return Response(content=path.read_bytes(), media_type="application/pdf")


@app.put("/api/projects/{project_id}/documents/{document_id}/fields")
async def update_fields(project_id: str, document_id: str, payload: dict):
    project = load_project(project_id)
    document = find_item(project.get("documents", []), document_id)
    if not document:
        raise HTTPException(status_code=404, detail="문서를 찾을 수 없습니다.")
    document["fields"] = geometry_fields(payload.get("fields") or [])
    save_project(project)
    return public_project(project)


@app.delete("/api/projects/{project_id}/documents/{document_id}")
def delete_document(project_id: str, document_id: str):
    project = load_project(project_id)
    project["documents"] = [doc for doc in project.get("documents", []) if doc.get("id") != document_id]
    for participant in project.get("participants", []):
        (participant.get("signatures") or {}).pop(document_id, None)
    pdf_file(project_id, document_id).unlink(missing_ok=True)
    save_project(project)
    return public_project(project)


@app.post("/api/projects/{project_id}/participants")
async def add_participant(project_id: str, payload: dict):
    project = load_project(project_id)
    name = str(payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="피험자 이름이 필요합니다.")
    project.setdefault("participants", []).append({
        "id": new_id("part"),
        "name": name,
        "createdAt": now_iso(),
        "confirmed": False,
        "confirmedAt": None,
        "signatures": {},
    })
    save_project(project)
    return public_project(project)


@app.post("/api/projects/{project_id}/participants/{participant_id}/confirm")
def confirm_participant(project_id: str, participant_id: str):
    project = load_project(project_id)
    participant = find_item(project.get("participants", []), participant_id)
    if not participant:
        raise HTTPException(status_code=404, detail="피험자를 찾을 수 없습니다.")
    participant["confirmed"] = True
    participant["confirmedAt"] = now_iso()
    save_project(project)
    return public_project(project)


@app.post("/api/session")
async def start_session(payload: dict):
    project = load_project(payload.get("projectId") or "")
    mode = payload.get("mode")
    if mode not in ("prep", "live"):
        raise HTTPException(status_code=400, detail="모드가 올바르지 않습니다.")
    document = find_item(project.get("documents", []), payload.get("documentId"))
    participant = None
    if mode == "live":
        participant = find_item(project.get("participants", []), payload.get("participantId"))
        if not participant:
            raise HTTPException(status_code=404, detail="피험자를 찾을 수 없습니다.")
    app_state["project_id"] = project["id"]
    app_state["mode"] = mode
    app_state["participant_id"] = participant["id"] if participant else None
    app_state["document_id"] = document["id"] if document else None
    app_state["document_name"] = document["name"] if document else ""
    app_state["participant"] = {
        "name": participant["name"] if participant else "",
        "date": payload.get("date") or "",
    }
    app_state["fields"] = session_fields(project, app_state["participant_id"], app_state["document_id"]) if document else []
    app_state["regions"] = {}
    message = {
        "type": "SESSION",
        "mode": mode,
        "projectId": project["id"],
        "documentId": app_state["document_id"],
        "documentName": app_state["document_name"],
        "participant": app_state["participant"],
        "fields": app_state["fields"],
    }
    await manager.broadcast(message)
    return message


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    await websocket.send_json({"type": "INIT_STATE", "data": app_state})
    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "UPDATE_FIELDS":
                app_state["fields"] = data.get("fields", [])
                await manager.broadcast({"type": "FIELDS_UPDATED", "fields": app_state["fields"]})

            elif msg_type == "SUBMIT_SIGNATURE":
                field_id = data.get("field_id")
                img_data = data.get("image_data")
                for field in app_state["fields"]:
                    if field["id"] == field_id:
                        field["signed"] = True
                        field["img"] = img_data
                        break
                persist_signature(field_id, img_data)
                await manager.broadcast({
                    "type": "SIGNATURE_RECEIVED",
                    "field_id": field_id,
                    "image_data": img_data,
                    "fields": app_state["fields"],
                })

            elif msg_type == "UPDATE_PARTICIPANT":
                app_state["participant"] = data.get("participant")
                await manager.broadcast({"type": "PARTICIPANT_UPDATED", "participant": app_state["participant"]})

            elif msg_type == "UPDATE_REGIONS":
                app_state["regions"] = data.get("regions") or {}
                await manager.broadcast({"type": "REGIONS_UPDATED", "regions": app_state["regions"]})

    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/", response_class=HTMLResponse)
async def get_admin_page(request: Request):
    with open("templates/admin.html", "r", encoding="utf-8") as handle:
        return handle.read()


@app.get("/pad", response_class=HTMLResponse)
async def get_pad_page(request: Request):
    with open("templates/pad.html", "r", encoding="utf-8") as handle:
        return handle.read()


if __name__ == "__main__":
    local_ip = get_local_ipv4()
    print("==================================================")
    print("IRB PDF Server starting...")
    print(f"PC Admin URL:  http://localhost:8000")
    print(f"iPad Pad URL:   http://{local_ip}:8000/pad")
    print(f"IPv4:           {local_ip}")
    print("==================================================")
    uvicorn.run(app, host="0.0.0.0", port=8000)
