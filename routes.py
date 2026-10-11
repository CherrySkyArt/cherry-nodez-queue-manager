import re

from aiohttp import web
from server import PromptServer

from .cherry_queue import manager
from .persistence import is_job_id


async def _read_json(request):
    try:
        body = await request.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _bad_request(message):
    return web.json_response({"error": message}, status=400)


def _job_ids(body):
    """None means every job; raises ValueError on anything that isn't {"all": true} or {"ids": [...]}."""
    if body.get("all") is True:
        return None
    ids = body.get("ids")
    if isinstance(ids, list) and ids and all(is_job_id(i) for i in ids):
        return ids
    raise ValueError('send {"all": true} or {"ids": [job ids]}')


def _safe_filename(name):
    cleaned = re.sub(r'[<>:"/\\|?*%\x00-\x1f]', "", name).strip().rstrip(".")
    return cleaned[:80] or "Job"


@PromptServer.instance.routes.get("/cherry_queue/state")
async def get_state(request):
    return web.json_response(manager.state())


@PromptServer.instance.routes.post("/cherry_queue/pause")
async def set_paused(request):
    body = await _read_json(request)
    paused = body.get("paused")
    if not isinstance(paused, bool):
        return _bad_request("paused must be true or false")
    client_id = body.get("client_id")
    manager.set_paused(paused, client_id if isinstance(client_id, str) else None)
    return web.json_response(manager.state())


@PromptServer.instance.routes.post("/cherry_queue/move")
async def move_job(request):
    body = await _read_json(request)
    job_id, before = body.get("id"), body.get("before")
    if not is_job_id(job_id) or (before is not None and not is_job_id(before)):
        return _bad_request("id and before must be job ids")
    return web.json_response({"moved": manager.move(job_id, before)})


@PromptServer.instance.routes.post("/cherry_queue/rename")
async def rename_job(request):
    body = await _read_json(request)
    job_id, name = body.get("id"), body.get("name")
    if not is_job_id(job_id) or not isinstance(name, str):
        return _bad_request("id must be a job id and name a string")
    return web.json_response({"renamed": manager.rename(job_id, name)})


@PromptServer.instance.routes.post("/cherry_queue/dismiss")
async def dismiss_restore(request):
    manager.dismiss_restore()
    return web.json_response(manager.state())


@PromptServer.instance.routes.post("/cherry_queue/save_list")
async def save_to_list(request):
    try:
        ids = _job_ids(await _read_json(request))
    except ValueError as e:
        return _bad_request(str(e))
    return web.json_response({"saved": manager.save_to_list(ids)})


@PromptServer.instance.routes.post("/cherry_queue/save_workflows")
async def save_workflows(request):
    try:
        ids = _job_ids(await _read_json(request))
    except ValueError as e:
        return _bad_request(str(e))
    exports, skipped = manager.workflows(ids)
    files = [{"filename": _safe_filename(name) + ".json", "workflow": workflow} for name, workflow in exports]
    return web.json_response({"files": files, "skipped": skipped})


@PromptServer.instance.routes.post("/cherry_queue/held/release")
async def release_held(request):
    body = await _read_json(request)
    job_id, client_id = body.get("id"), body.get("client_id")
    if not is_job_id(job_id):
        return _bad_request("id must be a job id")
    error = await manager.release(job_id, client_id if isinstance(client_id, str) else None)
    if error:
        return _bad_request(error)
    return web.json_response({"released": True})


@PromptServer.instance.routes.post("/cherry_queue/held/remove")
async def remove_held(request):
    body = await _read_json(request)
    job_id = body.get("id")
    if not is_job_id(job_id):
        return _bad_request("id must be a job id")
    return web.json_response({"removed": manager.remove_held(job_id)})


@PromptServer.instance.routes.post("/cherry_queue/saved/workflow")
async def saved_workflow(request):
    body = await _read_json(request)
    entry_id = body.get("id")
    if not is_job_id(entry_id):
        return _bad_request("id must be a saved job id")
    name, workflow, error = manager.saved_workflow(entry_id)
    if error:
        return _bad_request(error)
    return web.json_response({"filename": _safe_filename(name) + ".json", "workflow": workflow})


@PromptServer.instance.routes.post("/cherry_queue/saved/requeue")
async def requeue_saved(request):
    body = await _read_json(request)
    entry_id, client_id = body.get("id"), body.get("client_id")
    if not is_job_id(entry_id):
        return _bad_request("id must be a saved job id")
    error = await manager.requeue_saved(entry_id, client_id if isinstance(client_id, str) else None)
    if error:
        return _bad_request(error)
    return web.json_response({"queued": True})


@PromptServer.instance.routes.post("/cherry_queue/saved/rename")
async def rename_saved(request):
    body = await _read_json(request)
    entry_id, name = body.get("id"), body.get("name")
    if not is_job_id(entry_id) or not isinstance(name, str):
        return _bad_request("id must be a saved job id and name a string")
    return web.json_response({"renamed": manager.rename_saved(entry_id, name)})


@PromptServer.instance.routes.post("/cherry_queue/saved/delete")
async def delete_saved(request):
    body = await _read_json(request)
    entry_id = body.get("id")
    if not is_job_id(entry_id):
        return _bad_request("id must be a saved job id")
    return web.json_response({"deleted": manager.delete_saved(entry_id)})
