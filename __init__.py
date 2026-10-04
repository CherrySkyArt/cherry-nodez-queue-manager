from typing_extensions import override
from comfy_api.latest import ComfyExtension, io

from . import routes  # side effect: registers /cherry_queue/* routes and hooks ComfyUI's queue

WEB_DIRECTORY = "./web"


class CherryQueueExtension(ComfyExtension):
    @override
    async def get_node_list(self) -> list[type[io.ComfyNode]]:
        return []


async def comfy_entrypoint() -> CherryQueueExtension:
    return CherryQueueExtension()
