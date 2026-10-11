import copy
import heapq
import logging
import os
import time
import uuid

import execution
from server import PromptServer

from .persistence import LOG_PREFIX, QueueStore, SavedStore

STATE_EVENT = "cherry_queue.state"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_NUMBER_KEY = "cherry_job_number"
JOB_NAME_KEY = "cherry_job_name"
HOLD_KEY = "cherry_hold"  # set in extra_data by the sidebar's "queue on hold" button
MAX_NAME_LENGTH = 100


def _queue_order(item):
    return (item[0], item[1])


def _title(node):
    return (node.get("_meta") or {}).get("title") or node.get("class_type") or "?"


def job_label(prompt, extra_data, outputs):
    return {
        "number": extra_data.get(JOB_NUMBER_KEY),
        "name": extra_data.get(JOB_NAME_KEY),
        "outputs": ", ".join(sorted({_title(prompt[o]) for o in outputs if isinstance(prompt.get(o), dict)})),
    }


def _display_name(name, number):
    return name or f"Job {number or ''}".strip()


def _editor_workflow(extra_data):
    """The editor's copy of the workflow, with a fresh id so it opens as its own workflow; None if queued without one."""
    pnginfo = extra_data.get("extra_pnginfo")
    workflow = pnginfo.get("workflow") if isinstance(pnginfo, dict) else None
    if not isinstance(workflow, dict):
        return None
    workflow = copy.deepcopy(workflow)
    workflow["id"] = str(uuid.uuid4())
    return workflow


def _validation_message(result):
    _, error, _, node_errors = result
    for node_error in (node_errors or {}).values():
        for err in node_error.get("errors", []):
            message = f"{node_error.get('class_type', 'Node')}: {err.get('message') or 'invalid input'}"
            if err.get("details"):
                message += f" ({err['details']})"
            return message[:300]
    if isinstance(error, dict):
        message = error.get("message") or "Invalid workflow"
        if error.get("details"):
            message += f" ({error['details']})"
        return message[:300]
    return str(error)[:300]


class QueueManager:
    def __init__(self, server):
        self.server = server
        self.queue = server.prompt_queue
        self.store = QueueStore(DATA_DIR)
        self.saved = SavedStore(os.path.join(DATA_DIR, "saved"))
        # Jobs on hold live here, outside ComfyUI's queue, so the worker never sees them; guarded by queue.mutex.
        self.held = []
        self._saved_jobs = self.store.load()
        saved_numbers = [job["extra_data"].get(JOB_NUMBER_KEY) for _, _, _, job, _ in self._saved_jobs if job]
        self._next_number = max([self.store.next_number] + [n + 1 for n in saved_numbers if isinstance(n, int)])
        # Restored jobs wait for the user: a job that crashed ComfyUI would otherwise crash it again on every start.
        self.paused = any(not on_hold for _, _, on_hold, _, _ in self._saved_jobs)
        self._restored_ids = set()
        self._interrupted_id = None
        self._failed = []
        self._wrap_queue()
        server.app.on_startup.append(self._restore_on_startup)

    def _wrap_queue(self):
        queue = self.queue
        original_get = queue.get
        original_put = queue.put
        original_queue_updated = self.server.queue_updated

        def get(timeout=None):
            with queue.mutex:
                while self.paused or not queue.queue:
                    queue.not_empty.wait(timeout=timeout)
                    if timeout is not None and (self.paused or not queue.queue):
                        return None
                # Still holding the (reentrant) lock, so nothing can change between this check and the pop.
                return original_get(timeout=timeout)

        def put(item):
            with queue.mutex:
                extra_data = item[3]
                on_hold = False
                if isinstance(extra_data, dict):
                    on_hold = bool(extra_data.pop(HOLD_KEY, False))
                    if not isinstance(extra_data.get(JOB_NUMBER_KEY), int):
                        extra_data[JOB_NUMBER_KEY] = self._next_number
                        self._next_number += 1
                if not on_hold:
                    original_put(item)
                    return
                self.held.append(item)
            self.server.queue_updated()

        def queue_updated():
            original_queue_updated()
            self.store.request_save()

        queue.get = get
        queue.put = put
        self.server.queue_updated = queue_updated

    async def _restore_on_startup(self, app):
        # Runs after every custom node has loaded, so saved jobs validate against the full node list.
        try:
            await self._restore()
        except Exception:
            logging.exception(f"{LOG_PREFIX} Restoring the saved queue failed; saved files are untouched and saving is off until restart")
            return
        self.store.start_saving(self.snapshot)

    async def _restore(self):
        saved, self._saved_jobs = self._saved_jobs, []
        if not saved:
            return
        for job_id, was_running, on_hold, job, error in saved:
            if error is None and on_hold:
                # Not validated until released, since that's when it would run.
                with self.queue.mutex:
                    self.held.append((0, job_id, job["prompt"], job["extra_data"], job["outputs_to_execute"], {}))
                continue
            if error is None:
                error = await self._requeue(job_id, job)
            if error:
                label = job_label(job["prompt"], job["extra_data"], job["outputs_to_execute"]) if job else {"number": None, "outputs": ""}
                self._failed.append({"id": job_id, "label": label, "error": error})
                self.store.move_to_failed(job_id)
                continue
            self._restored_ids.add(job_id)
            if was_running and self._interrupted_id is None:
                self._interrupted_id = job_id
        if not self._restored_ids:
            with self.queue.mutex:
                self.paused = False
        logging.info(f"{LOG_PREFIX} Restored {len(self._restored_ids)} job(s) and {len(self.held)} on hold, "
                     f"{len(self._failed)} could not be restored" + ("; the queue is paused" if self.paused else ""))

    async def _validate(self, job_id, prompt, outputs):
        """(None, outputs to execute) if the job can run, else (error message, None)."""
        try:
            result = await execution.validate_prompt(job_id, prompt, outputs or None)
        except Exception as e:
            return f"Validation crashed: {e}", None
        if not result[0]:
            return _validation_message(result), None
        return None, result[2]

    def _put_last(self, item):
        number = self.server.number
        self.server.number += 1
        self.queue.put((number,) + tuple(item[1:]))

    async def _requeue(self, job_id, job):
        error, outputs = await self._validate(job_id, job["prompt"], job["outputs_to_execute"])
        if error is None:
            self._put_last((None, job_id, job["prompt"], job["extra_data"], outputs, {}))
        return error

    def snapshot(self):
        with self.queue.mutex:
            running = list(self.queue.currently_running.values())
            pending = sorted(self.queue.queue, key=_queue_order)
            return running, pending, list(self.held), self._next_number

    def state(self):
        running, pending, held, _ = self.snapshot()
        labels = {item[1]: job_label(item[2], item[3], item[4]) for item in running + pending + held}
        restored = [item[1] for item in pending if item[1] in self._restored_ids]
        restore = None
        if restored or self._failed:
            restore = {
                "restored": len(restored),
                "interrupted": self._interrupted_id if self._interrupted_id in restored else None,
                "failed": self._failed,
            }
        return {
            "paused": self.paused,
            "restore": restore,
            "labels": labels,
            "held": [{"id": item[1], "create_time": item[3].get("create_time")} for item in held],
            "saved": self.saved.list(),
        }

    def _items(self, ids=None):
        running, pending, held, _ = self.snapshot()
        items = running + pending + held
        if ids is None:
            return items
        wanted = set(ids)
        return [item for item in items if item[1] in wanted]

    def save_to_list(self, ids=None):
        items = self._items(ids)
        # The list shows newest first; counting down keeps a "save all" batch in queue order at the top.
        newest = int(time.time() * 1000)
        for offset, item in enumerate(items):
            self.saved.add(item[2], item[3], item[4], item[3].get(JOB_NAME_KEY), item[3].get(JOB_NUMBER_KEY), newest - offset)
        self._notify()
        return len(items)

    def workflows(self, ids=None):
        """(name, editor workflow) for each job; jobs queued without an editor workflow (e.g. via the API) are skipped."""
        exports, skipped = [], []
        for item in self._items(ids):
            name = _display_name(item[3].get(JOB_NAME_KEY), item[3].get(JOB_NUMBER_KEY))
            workflow = _editor_workflow(item[3])
            if workflow is None:
                skipped.append(name)
            else:
                exports.append((name, workflow))
        return exports, skipped

    def saved_workflow(self, entry_id):
        """(name, editor workflow, error) for one Saved-list entry."""
        entry = self.saved.get(entry_id)
        if entry is None:
            return None, None, "That saved job no longer exists"
        name = _display_name(entry["name"], entry["number"])
        workflow = _editor_workflow(entry["extra_data"])
        if workflow is None:
            return name, None, f"{name} has no editor workflow to download (it was probably queued through the API)"
        return name, workflow, None

    async def requeue_saved(self, entry_id, client_id=None):
        entry = self.saved.get(entry_id)
        if entry is None:
            return "That saved job no longer exists"
        extra_data = entry["extra_data"]
        for key in (JOB_NUMBER_KEY, JOB_NAME_KEY, HOLD_KEY, "client_id"):
            extra_data.pop(key, None)
        extra_data["create_time"] = int(time.time() * 1000)
        if entry["name"]:
            extra_data[JOB_NAME_KEY] = entry["name"]
        if client_id:
            extra_data["client_id"] = client_id
        return await self._requeue(str(uuid.uuid4()), entry)

    def rename_saved(self, entry_id, name):
        renamed = self.saved.rename(entry_id, " ".join(name.split())[:MAX_NAME_LENGTH])
        self._notify()
        return renamed

    def delete_saved(self, entry_id):
        deleted = self.saved.delete(entry_id)
        self._notify()
        return deleted

    def _find_held(self, job_id):
        with self.queue.mutex:
            return next((item for item in self.held if item[1] == job_id), None)

    async def release(self, job_id, client_id=None):
        """Move a job from on hold to the end of the queue; it stays on hold if it no longer validates."""
        item = self._find_held(job_id)
        if item is None:
            return "That job is no longer on hold"
        error, outputs = await self._validate(job_id, item[2], item[4])
        if error:
            return error
        with self.queue.mutex:
            if not any(held is item for held in self.held):
                return "That job is no longer on hold"
            self.held = [held for held in self.held if held is not item]
            if client_id:
                item[3]["client_id"] = client_id  # progress and previews go to the tab that released it
            self._put_last((None, job_id, item[2], item[3], outputs, item[5]))
        return None

    def remove_held(self, job_id):
        with self.queue.mutex:
            before = len(self.held)
            self.held = [item for item in self.held if item[1] != job_id]
            removed = len(self.held) != before
        if removed:
            self.server.queue_updated()
        return removed

    def set_paused(self, paused, client_id=None):
        with self.queue.mutex:
            self.paused = paused
            if not paused:
                if client_id:
                    # Progress and previews go to the browser tab that queued a job; that tab's id died with the restart.
                    for item in self.queue.queue:
                        if item[1] in self._restored_ids:
                            item[3]["client_id"] = client_id
                self._restored_ids.clear()
                self._interrupted_id = None
                self.queue.not_empty.notify_all()
        self._notify()

    def move(self, job_id, before_id=None):
        with self.queue.mutex:
            pending = sorted(self.queue.queue, key=_queue_order)
            ids = [item[1] for item in pending]
            if job_id not in ids or job_id == before_id or (before_id is not None and before_id not in ids):
                return False
            numbers = [item[0] for item in pending]
            moved = pending.pop(ids.index(job_id))
            ids.remove(job_id)
            pending.insert(ids.index(before_id) if before_id is not None else len(pending), moved)
            # Reuse the existing numbers so new jobs (and "queue to front") still land where core puts them.
            self.queue.queue[:] = [(number,) + item[1:] for number, item in zip(numbers, pending)]
            heapq.heapify(self.queue.queue)
        self.server.queue_updated()
        return True

    def rename(self, job_id, name):
        name = " ".join(name.split())[:MAX_NAME_LENGTH]
        with self.queue.mutex:
            items = [item for item in self.queue.queue if item[1] == job_id]
            items += [item for item in self.queue.currently_running.values() if item[1] == job_id]
            items += [item for item in self.held if item[1] == job_id]
            if not items:
                return False
            for item in items:
                if name:
                    item[3][JOB_NAME_KEY] = name
                else:
                    item[3].pop(JOB_NAME_KEY, None)
            self.store.mark_dirty(job_id)
        self.server.queue_updated()
        return True

    def dismiss_restore(self):
        self._restored_ids.clear()
        self._interrupted_id = None
        self._failed = []
        self._notify()

    def _notify(self):
        self.server.send_sync(STATE_EVENT, {"paused": self.paused})


manager = QueueManager(PromptServer.instance)
