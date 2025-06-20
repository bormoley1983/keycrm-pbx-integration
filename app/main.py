"""FastAPI-based proxy between KeyCRM and FreePBX (Asterisk 19).

Key features
------------
* /calls             originate outbound call requested by KeyCRM
* /hangup/{id}       hang-up a running call
* AMI event loop     listens to Dial / Bridge / Cdr / Hangup events and
                     notifies KeyCRM about state changes
* Fully async        one asyncio loop, no threads; panoramisk handles AMI
* Minimal globals    active call registry lives in CallStore (dict wrapper)
* Typed models       Pydantic schemas for request/response and CDR payload
* Graceful stop      SIGTERM closes AMI and shuts down web-server
"""

import asyncio
import logging
import time
from contextlib import suppress, asynccontextmanager
from typing import Dict, Optional

import aiohttp
from fastapi import FastAPI, HTTPException, status
from panoramisk.manager import Manager
from pydantic import BaseModel, AnyHttpUrl, Field

from app.settings import settings  # see README or settings.py

import re
def normalize_dst(num: str) -> str:
    """
    Strip leading +38 or 38 from Ukrainian numbers.
    Examples:
        +380671234567 -> 0671234567
        380671234567  -> 0671234567
        0671234567    -> 0671234567  (unchanged)
    """
    return re.sub(r"^\+?38", "", num)

def to_international(num: str) -> str:
    """
    Convert a local Ukrainian number (e.g., 0671234567) to +380671234567.
    If already in international format, return as is.
    """
    if num.startswith("+38"):
        return num
    if num.startswith("38"):
        return "+" + num
    if num.startswith("0") and len(num) == 10:
        return "+38" + num
    return num  # fallback, return as is

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log = logging.getLogger("asterisk-proxy")
logging.basicConfig(level=settings.log_level,
                    format="%(asctime)s %(levelname)s %(name)s   %(message)s")

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class CallRequest(BaseModel):
    caller: str = Field(..., json_schema_extra={"example": "301"})
    destination_number: str = Field(..., json_schema_extra={"example": "0501234567"})
#    user_id: Optional[int] = Field(None, example=42)    # CRM user (optional)


class CallResponse(BaseModel):
    success: bool
    call_id: str


class CDR(BaseModel):
    call_id: str
    direction: str
    caller: str
    destination_number: str
    state: str
    duration: Optional[int] = None
    audio_url: Optional[AnyHttpUrl] = None


# ---------------------------------------------------------------------------
# In-memory call registry
# ---------------------------------------------------------------------------
class CallStore:
    """Keeps metadata for active calls."""
    def __init__(self) -> None:
        self._data: Dict[str, Dict] = {}

    def create(self, call_id: str, meta: Dict) -> None:
        self._data[call_id] = meta

    def update(self, call_id: str, **changes) -> None:
        if call_id in self._data:
            self._data[call_id].update(changes)

    def pop(self, call_id: str) -> Optional[Dict]:
        return self._data.pop(call_id, None)

    def get(self, call_id: str) -> Optional[Dict]:
        return self._data.get(call_id)

    def __len__(self) -> int:
        return len(self._data)


calls = CallStore()


# ---------------------------------------------------------------------------
#  Call correlation helpers
# ---------------------------------------------------------------------------
# Every call_id maps to {a_uid, b_uid, linkedid, channel}
id_store: dict[str, dict] = {}
# Quick reverse maps so we can resolve by Uniqueid or Linkedid
uid2id: dict[str, str] = {}
lid2id: dict[str, str] = {}


# ---------------------------------------------------------------------------
# FastAPI setup
# ---------------------------------------------------------------------------
app = FastAPI(title="KeyCRM to FreePBX proxy", version="1.0")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        app.state.ami = await connect_ami()
        app.state.listener = asyncio.create_task(ami_listener(app.state.ami))
        log.info("Service ready")
        yield
    except Exception as e:
        log.error("Failed to start AMI connection: %s", e)
        app.state.ami = None
        app.state.listener = None
        raise
    finally:
        # Shutdown logic (was @app.on_event("shutdown"))
        if hasattr(app.state, 'listener') and app.state.listener:
            app.state.listener.cancel()
            with suppress(asyncio.CancelledError):
                await app.state.listener
        if hasattr(app.state, 'ami') and app.state.ami:
            try:
                await app.state.ami.close()
            except Exception as e:
                log.warning("Error closing AMI connection: %s", e)
        log.info("Shutdown complete")

app = FastAPI(title="KeyCRM to FreePBX proxy", version="1.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Core handlers (business logic)
# ---------------------------------------------------------------------------
@app.post("/calls", response_model=CallResponse)
async def initiate_call(req: CallRequest):
    dst = normalize_dst(req.destination_number)
    call_id = f"{int(time.time())}_{req.caller}_{dst}"
    meta = {
        "direction": "outgoing",
        "caller": req.caller,
        "destination_number": dst,
        "original_destination_number": req.destination_number,
        "start_time": time.time(),
        "state": "started",
    }

    # build Originate AMI action -------------------------------------------
    action_id = f"orig_{call_id}"
    action = {
        "Action": "Originate",
        "Channel": f"PJSIP/{req.caller}",
        "Exten": dst,
        "Context": "from-internal",
        "Priority": "1",
        "CallerID": req.caller,
        "Variable": f"__KEYCRM_CALL_ID={call_id}",
        "Async": "true",
        "Timeout": "30000",
        "ActionID": action_id,
    }

    # house-keeping maps ----------------------------------------------------
    id_store[call_id] = {"action_id": action_id, "caller": req.caller}
    calls.create(call_id, meta)

    # send web-hook and Originate ------------------------------------------
    await notify_keycrm(call_id, **meta)
    await app.state.ami.send_action(action)

    return CallResponse(success=True, call_id=call_id)


@app.post("/hangup/{call_id}")
async def hangup_call(call_id: str):
    # 1. look up our metadata
    meta = id_store.get(call_id) or calls.get(call_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Unknown call")

    # 2. Pick B-leg first, then A-leg
    b_uid = meta.get("b_uid")
    a_uid = meta.get("a_uid")
    if b_uid:
        action = {"Action": "Hangup", "Uniqueid": b_uid}
    elif a_uid:
        action = {"Action": "Hangup", "Uniqueid": a_uid}
    elif ch := meta.get("channel"):
        # fallback to channel string if no UID yet
        action = {"Action": "Hangup", "Channel": ch}
    else:
        # last resort: raw extension
        action = {"Action": "Hangup", "Channel": f"PJSIP/{meta['caller']}"}

    # 3. hang up *strictly* by Uniqueid
    await app.state.ami.send_action(action)
    log.info("Hangup action sent: %r for call %s", action, call_id)
    return {"success": True}
    
    
# ---------------------------------------------------------------------------
# HTTP endpoints
# ---------------------------------------------------------------------------
@app.post("/api/initiate_call")
async def keycrm_initiate(payload: dict):
    """
    KeyCRM sends: {"caller": "301", "destination_number": "380730000001"}
    """
    req = CallRequest(**payload)        # re-use the pydantic model
    return await initiate_call(req)


@app.post("/api/hangup_call")
async def keycrm_hangup(payload: dict):
    """
    KeyCRM sends the same structure when the manager presses 'End call'.
    We look up the last active call for that caller (or use the optional id).
    """
    log.debug("KeyCRM hangup payload: %r", payload)

    # 1. If KeyCRM wrapped under "call", unwrap it
    data = payload.get("call", payload)
    # 2. Accept explicit "id" or "call_id", or fall back to "caller"
    call_id = data.get("id") or data.get("call_id")
    caller  = data.get("caller")

    # 3. If no explicit call_id, try to find by caller
    if not call_id and caller:
        for cid, meta in reversed(list(calls._data.items())):
            if meta.get("caller") == caller:
                call_id = cid
                break

    if not call_id:
        # If we can't find the call, it might have already completed
        # This is normal behavior - return success instead of 404
        log.info("KeyCRM hangup request for already completed call: %r", payload)
        return {"success": True, "message": "Call already completed"}

    # 4. Try to hangup the call (it might already be hung up)
    try:
        return await hangup_call(call_id)
    except HTTPException as e:
        if e.status_code == 404:
            # Call already completed, return success
            log.info("KeyCRM hangup request for already completed call: %s", call_id)
            return {"success": True, "message": "Call already completed"}
        else:
            raise


@app.get("/api/healthstate")
async def health():
    return {
        "status": "ok",
        "active_calls": len(calls),
        "ami_connected": bool(
            getattr(app.state.ami, "connected", None)
            or getattr(app.state.ami, "_connected", None)
        ),
    }


@app.get("/api/debug/calls")
async def debug_calls():
    """Debug endpoint to view current call state."""
    return {
        "active_calls": calls._data,
        "id_store": id_store,
        "uid2id_count": len(uid2id),
        "lid2id_count": len(lid2id),
        "completed_calls_count": len(_completed_calls)
    }


# ---------------------------------------------------------------------------
# Asterisk AMI helpers
# ---------------------------------------------------------------------------
async def connect_ami() -> Manager:
    ami = Manager(
        host=settings.asterisk_host,
        port=settings.asterisk_port,
        username=settings.ami_user,
        secret=settings.ami_secret,
        loop=asyncio.get_running_loop(),
        reconnect_timeout=5,
    )
    await ami.connect()
    return ami


async def originate_via_ami(ami: Manager, call_id: str, caller: str, destination: str):
    action = {
        "Action": "Originate",
        "Channel": f"PJSIP/{caller}",
        "Exten": destination,
        "Context": "from-internal",
        "Priority": "1",
        "CallerID": f"{caller}",
        "Variable": f"__KEYCRM_CALL_ID={call_id}",
        "Async": "true",
        "Timeout": "30000",
    }
    await ami.send_action(action)
    log.info("Originate queued: %s → %s (%s)", caller, destination, call_id)


async def ami_listener(ami: Manager):
    """Background task: route AMI events to handlers."""
    
    @ami.register_event("OriginateResponse")
    async def or_handler(mgr, evt):
        aid = evt.get("ActionID")
        uid = evt.get("Uniqueid")
        lid = evt.get("Linkedid") or uid
        ch  = evt.get("Channel")
        response = evt.get("Response", "Unknown")
        
        log.debug("OriginateResponse: ActionID=%s, Uniqueid=%s, Linkedid=%s, Response=%s", aid, uid, lid, response)
        
        # Find the matching call by ActionID
        cid = None
        for call_id, meta in id_store.items():
            if meta.get("action_id") == aid:
                cid = call_id
                break
        
        if not cid:
            log.warning("OriginateResponse: No call found for ActionID=%s", aid)
            return
        
        # Check if originate was successful
        if response != "Success":
            log.warning("OriginateResponse failed for call %s: %s", cid, response)
            # Mark call as failed and clean up
            calls.update(cid, state="failed")
            meta = calls.get(cid)
            if meta:
                await notify_keycrm(cid, **meta, state="failed")
            else:
                log.warning(f"No call metadata found for failed call {cid}, skipping notify_keycrm.")
                #return  # Do not proceed further if meta is None
            # Clean up failed call
            calls.pop(cid)
            id_store.pop(cid, None)
            return
        
        # Ensure we have valid identifiers
        if not uid:
            log.error("OriginateResponse: Missing Uniqueid for call %s", cid)
            return
        
        # Update the call mapping with Asterisk identifiers
        meta = id_store[cid]
        meta.update(a_uid=uid, linkedid=lid, channel=ch)
          # Add to reverse lookup maps
        uid2id[uid] = cid
        if lid and lid != uid:  # Only add if linkedid is different from uniqueid
            lid2id[lid] = cid
        
        log.info("Successfully mapped call %s: uid=%s, lid=%s, channel=%s", cid, uid, lid, ch)
    
    @ami.register_event("DialBegin")
    async def dial_handler(mgr, evt):
        lid = evt.get("Linkedid") or evt.get("Uniqueid")
        cid = lid2id.get(lid)
        
        log.debug("DialBegin: Linkedid=%s, call_id=%s", lid, cid)
        
        if cid:
            b_uid = evt.get("DestUniqueid")
            meta = id_store[cid]
            meta.update(b_uid=b_uid)
            uid2id[b_uid] = cid
            calls.update(cid, state="connected")
            log.debug("Call %s connected: b_uid=%s", cid, b_uid)
            meta = calls.get(cid)
            if meta:
                await notify_keycrm(cid, **meta)
            else:
                log.warning(f"No call metadata found for connected call {cid}, skipping notify_keycrm.")
                #return  # Do not proceed further if meta is None
    
    @ami.register_event("Hangup")
    async def hang_handler(mgr, evt):
        uid = evt.get("Uniqueid")
        lid = evt.get("Linkedid")
        
        # Find call ID by checking both Uniqueid and Linkedid
        cid = uid2id.get(uid) or lid2id.get(lid)
        if not cid:
            log.debug("Hangup event for unknown call: uid=%s, lid=%s", uid, lid)
            return
        
        log.debug("Hangup event for call %s: uid=%s, lid=%s", cid, uid, lid)
        
        # Get call metadata
        call_meta = calls.get(cid)
        id_meta = id_store.get(cid, {})
        
        if not call_meta and not id_meta:
            log.debug("No metadata found for call %s", cid)
            return
        
        # Check if this is the first hangup for this call
        # (calls can have multiple channels that hang up separately)
        if cid in calls._data:
            # This is the first hangup - send completion notification
            meta = calls.pop(cid) or {}
            meta.update(id_meta)
            
            # Calculate duration
            start_time = meta.get("start_time", time.time())
            dur = int(time.time() - start_time)
            
            # Remove state from meta and add completed state with duration
            meta.pop("state", None)
            
            log.info("Call %s completed, duration: %d seconds", cid, dur)
            if meta:
                await notify_keycrm(cid, **meta, state="completed", duration=dur)
            else:
                log.warning(f"No call metadata found for completed call {cid}, skipping notify_keycrm.")
                #return  # Do not proceed further if meta is None
        
        # Clean up tracking dictionaries for this specific channel
        if uid and uid in uid2id and uid2id[uid] == cid:
            del uid2id[uid]
        if lid and lid in lid2id and lid2id[lid] == cid:
            del lid2id[lid]
        # Clean up id_store only after all channels are done
        # Check if there are any remaining references to this call
        remaining_refs = any(
            call_id == cid for call_id in list(uid2id.values()) + list(lid2id.values())
        )
        if not remaining_refs and cid in id_store:
            del id_store[cid]
            
        log.debug("Fully cleaned up call %s from tracking", cid)
    
    @ami.register_event("Newchannel")
    async def on_new_inbound(mgr, evt):
        if evt.get("Context") != "from-pstn":
            return                        # skip outbound and local channels

        uid  = evt.get("Uniqueid")
        lid  = evt.get("Linkedid") or uid
        cid  = f"in_{uid}"               # own id scheme
        ani  = evt.get("CallerIDNum")
        did  = evt.get("Exten")          # the DID that was dialled

        if cid in id_store:              # duplicate safety
            return

        # store maps so later DialBegin / Hangup resolve to the same cid
        id_store[cid] = {
            "a_uid": uid,
            "linkedid": lid,
            "customer_number": ani,  # external number
            "pbx_extension": did,    # internal extension (DID)
            "direction": "incoming",
            "start_time": time.time(),
            "state": "started",
        }
        uid2id[uid] = cid
        lid2id[lid] = cid
        calls.create(cid, id_store[cid])

        # notify CRM immediately so the agent popup appears
        meta = calls.get(cid)
        if meta:
            await notify_keycrm(cid, **meta)

    # actively wait until the AMI socket finishes the login handshake
    while not getattr(ami, 'connected', getattr(ami, '_connected', False)):
        await asyncio.sleep(0.5)
    # once connected, park the coroutine forever so handlers stay live
    while True:
        await asyncio.sleep(3600)  
        

# ---------------------------------------------------------------------------
# Webhook to KeyCRM
# ---------------------------------------------------------------------------

# Track completed calls to avoid duplicates
_completed_calls = set()

def get_crm_fields(meta):
    """
    Determines the caller (internal extension) and destination (customer number)
    for the KeyCRM webhook based on the call direction.
    """
    direction = meta.get("direction")
    
    if direction == "outgoing":
        # For outgoing calls:
        # - The 'caller' is the internal PBX extension initiating the call.
        # - The 'destination_number' is the external customer's number.
        caller = meta.get("caller")
        destination_number = meta.get("original_destination_number") or meta.get("destination_number")
    else:  # incoming
        # For incoming calls:
        # - The 'caller' for KeyCRM should be the internal PBX extension that was called.
        # - The 'destination_number' is the external customer's number who initiated the call.
        caller = meta.get("pbx_extension")
        destination_number = meta.get("customer_number")
        
    return caller, destination_number

async def notify_keycrm(call_id: str, **meta):
    if not settings.keycrm_webhook_url:
        log.warning("KeyCRM webhook URL not configured, skipping notification")
        return
        
    # Filter out invalid calls (missing caller or destination)
    caller, destination_number = get_crm_fields(meta)
    destination_number = to_international(destination_number or "")
    state = str(meta.get("state") or "unknown")
    
    # Skip calls with invalid/missing data
    if not caller or caller == "<unknown>" or not destination_number or destination_number == "s":
        log.debug("Skipping KeyCRM notification for invalid call: caller=%s, dest=%s", caller, destination_number)
        return
    
    # Map internal states to KeyCRM states
    state_mapping = {
        "started": "started",
        "connected": "answered",  # KeyCRM uses "answered" for connected calls
        "completed": "completed",
        "canceled": "canceled",
        "busy": "busy",
        "transferred": "transferred"
    }
    
    keycrm_state = state_mapping.get(state, state)
    
    # Prevent duplicate "completed" notifications
    if keycrm_state == "completed":
        completed_key = f"{call_id}_completed"
        if completed_key in _completed_calls:
            log.debug("Skipping duplicate completed notification for call %s", call_id)
            return
        _completed_calls.add(completed_key)
    
    # Build clean payload
    call_data = {
        "id": call_id,
        "direction": meta.get("direction"),
        "caller": caller,
        "destination_number": destination_number,
        "state": keycrm_state
    }
    
    # Only include duration and audio_url for completed calls
    if keycrm_state == "completed":
        if "duration" in meta:
            call_data["duration"] = meta["duration"]
        if "audio_url" in meta:
            call_data["audio_url"] = meta["audio_url"]
    
    # Remove None values to keep payload clean
    call_data = {k: v for k, v in call_data.items() if v is not None}
    
    payload = {"action": "call", "call": call_data}
    
    log.debug("Sending to KeyCRM: %r", payload)
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
            async with s.post(settings.keycrm_webhook_url, json=payload) as resp:
                if resp.status >= 300:
                    log.warning("KeyCRM %s %s", resp.status, await resp.text())
                else:
                    log.debug("Webhook OK   %s %s", call_id, keycrm_state)
    except asyncio.TimeoutError:
        log.error("Timeout sending webhook to KeyCRM for call %s", call_id)
    except Exception as e:
        log.error("Error sending webhook to KeyCRM for call %s: %s", call_id, e)