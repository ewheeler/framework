from __future__ import annotations
import io
from typing import TYPE_CHECKING
from ...system import Plugin
from .control import StreamControl
from .loader import StreamLoader

if TYPE_CHECKING:
    from ...resource import Resource


class StreamPlugin(Plugin):
    """Plugin for Stream Data"""

    # Hooks

    def create_loader(self, resource):
        if resource.scheme == "stream":
            return StreamLoader(resource)

    def detect_resource(self, resource: Resource):
        if resource.data is not None:
            if hasattr(resource.data, "read"):
                resource.scheme = "stream"
        elif resource.scheme == "stream":
            resource.data = io.BufferedRandom(io.BytesIO())  # type: ignore

    def select_control_class(self, type):
        if type == "stream":
            return StreamControl
