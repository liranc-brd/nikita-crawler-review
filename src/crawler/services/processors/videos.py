from __future__ import annotations

import os
from tempfile import NamedTemporaryFile
from typing import Any

from pymediainfo import MediaInfo


class VideoProcessor:
    content_types = ("video/mp4", "video/webm", "video/quicktime")
    metadata_type = "video"

    def extract_metadata(self, body: bytes, headers: dict[str, str]) -> dict[str, Any]:
        return {"file_size": len(body), "duration": self._duration_seconds(body)}

    @staticmethod
    def _duration_seconds(body: bytes) -> float | None:
        temporary_file = NamedTemporaryFile(delete=False)
        try:
            temporary_file.write(body)
            temporary_file.close()
            media_info = MediaInfo.parse(temporary_file.name)
            general_track = next(
                (track for track in media_info.tracks if track.track_type == "General"),
                None,
            )
            if general_track is None or general_track.duration is None:
                return None
            return float(general_track.duration) / 1000
        except Exception:
            return None
        finally:
            temporary_file.close()
            os.unlink(temporary_file.name)
