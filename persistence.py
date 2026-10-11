import json
import logging
import os
import threading
import time
import uuid

LOG_PREFIX = "[cherry-queue]"


def is_job_id(value):
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value
    except ValueError:
        return False


def _write_json_atomic(path, data):
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp_path, path)


def _parse_job(data):
    if not isinstance(data, dict) or not isinstance(data.get("prompt"), dict):
        raise ValueError("not a saved job")
    extra_data = data.get("extra_data")
    outputs = data.get("outputs_to_execute")
    return {
        "prompt": data["prompt"],
        "extra_data": extra_data if isinstance(extra_data, dict) else {},
        "outputs_to_execute": outputs if isinstance(outputs, list) else [],
    }


class QueueStore:
    """index.json holds the queue order; jobs/<id>.json holds one job each and is written only once."""

    def __init__(self, root):
        self.index_path = os.path.join(root, "index.json")
        self.jobs_dir = os.path.join(root, "jobs")
        self.failed_dir = os.path.join(root, "failed")
        os.makedirs(self.jobs_dir, exist_ok=True)
        self.next_number = 1
        self._written = set()
        self._wake = threading.Event()

    def _job_path(self, job_id):
        return os.path.join(self.jobs_dir, job_id + ".json")

    def _read_index(self):
        try:
            with open(self.index_path, encoding="utf-8") as f:
                data = json.load(f)
            entries = [entry for entry in data["jobs"] if isinstance(entry, dict)]
        except FileNotFoundError:
            return None
        except (OSError, ValueError, KeyError, TypeError) as e:
            logging.warning(f"{LOG_PREFIX} {self.index_path} is unreadable ({e}); recovering jobs from {self.jobs_dir}")
            return None
        if isinstance(data.get("next_number"), int):
            self.next_number = data["next_number"]
        return entries

    def load(self):
        """Saved jobs in queue order, as (job_id, was_running, on_hold, job or None, error or None)."""
        index = self._read_index()
        recovering = index is None
        if recovering:
            index = [{"id": name[:-5]} for name in os.listdir(self.jobs_dir) if name.endswith(".json")]
        saved = []
        for entry in index:
            job_id = entry.get("id")
            if not is_job_id(job_id):
                continue
            was_running, on_hold = bool(entry.get("running")), bool(entry.get("held"))
            try:
                with open(self._job_path(job_id), encoding="utf-8") as f:
                    job = _parse_job(json.load(f))
            except (OSError, ValueError) as e:
                saved.append((job_id, was_running, on_hold, None, f"Saved job file is unreadable: {e}"))
                continue
            self._written.add(job_id)
            saved.append((job_id, was_running, on_hold, job, None))
        if recovering:
            saved.sort(key=lambda s: s[3]["extra_data"].get("create_time", 0) if s[3] else 0)
        return saved

    def save(self, running, pending, held, next_number):
        keep, index = set(), []
        tagged = [(item, "running") for item in running] + [(item, "pending") for item in pending] + [(item, "held") for item in held]
        for item, status in tagged:
            job_id = item[1]
            if job_id in keep or not is_job_id(job_id):
                continue
            if job_id not in self._written:
                try:
                    _write_json_atomic(self._job_path(job_id), {"prompt": item[2], "extra_data": item[3], "outputs_to_execute": item[4]})
                except (OSError, TypeError, ValueError) as e:
                    logging.warning(f"{LOG_PREFIX} Could not save job {job_id}: {e}")
                    continue
                self._written.add(job_id)
            keep.add(job_id)
            index.append({"id": job_id, "running": status == "running", "held": status == "held"})
        _write_json_atomic(self.index_path, {"version": 1, "next_number": next_number, "jobs": index})
        for name in os.listdir(self.jobs_dir):
            if name.endswith(".json") and name[:-5] not in keep:
                try:
                    os.remove(os.path.join(self.jobs_dir, name))
                except OSError:
                    continue
                self._written.discard(name[:-5])

    def move_to_failed(self, job_id):
        os.makedirs(self.failed_dir, exist_ok=True)
        try:
            os.replace(self._job_path(job_id), os.path.join(self.failed_dir, job_id + ".json"))
        except OSError:
            pass
        self._written.discard(job_id)

    def mark_dirty(self, job_id):
        self._written.discard(job_id)

    def request_save(self):
        self._wake.set()

    def start_saving(self, snapshot):
        def loop():
            while True:
                self._wake.wait()
                time.sleep(0.25)  # fold bursts of queue changes into one write
                self._wake.clear()
                try:
                    self.save(*snapshot())
                except Exception:
                    logging.exception(f"{LOG_PREFIX} Saving the queue failed")

        threading.Thread(target=loop, name="cherry-queue-saver", daemon=True).start()
        self._wake.set()


class SavedStore:
    """saved/<id>.json: copies of jobs the user saved to re-queue later, independent of the live queue."""

    def __init__(self, root):
        self.dir = root
        os.makedirs(root, exist_ok=True)
        self._meta = {}
        for name in os.listdir(root):
            entry_id = name[:-5]
            if not name.endswith(".json") or not is_job_id(entry_id):
                continue
            try:
                self._meta[entry_id] = self._summary(entry_id, self._read(entry_id))
            except (OSError, ValueError) as e:
                logging.warning(f"{LOG_PREFIX} Skipping unreadable saved job {name}: {e}")

    def _path(self, entry_id):
        return os.path.join(self.dir, entry_id + ".json")

    def _read(self, entry_id):
        with open(self._path(entry_id), encoding="utf-8") as f:
            data = json.load(f)
        entry = _parse_job(data)
        entry["name"] = data["name"] if isinstance(data.get("name"), str) else None
        entry["number"] = data["number"] if isinstance(data.get("number"), int) else None
        entry["saved_at"] = data["saved_at"] if isinstance(data.get("saved_at"), int) else 0
        return entry

    @staticmethod
    def _summary(entry_id, entry):
        return {"id": entry_id, "name": entry["name"], "number": entry["number"], "saved_at": entry["saved_at"]}

    def _write(self, entry_id, entry):
        _write_json_atomic(self._path(entry_id), entry)
        self._meta[entry_id] = self._summary(entry_id, entry)

    def add(self, prompt, extra_data, outputs, name, number, saved_at):
        entry_id = str(uuid.uuid4())
        self._write(entry_id, {
            "name": name,
            "number": number,
            "saved_at": saved_at,
            "prompt": prompt,
            "extra_data": extra_data,
            "outputs_to_execute": outputs,
        })
        return entry_id

    def list(self):
        return sorted(self._meta.values(), key=lambda meta: meta["saved_at"], reverse=True)

    def get(self, entry_id):
        return self._read(entry_id) if entry_id in self._meta else None

    def rename(self, entry_id, name):
        entry = self.get(entry_id)
        if entry is None:
            return False
        entry["name"] = name or None
        self._write(entry_id, entry)
        return True

    def delete(self, entry_id):
        if self._meta.pop(entry_id, None) is None:
            return False
        try:
            os.remove(self._path(entry_id))
        except FileNotFoundError:
            pass
        return True
