from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator, TextIO
import zipfile


@dataclass(frozen=True)
class Member:
    """A normalized source member."""

    path: PurePosixPath
    size: int


class DataSource:
    """Expose file members without extracting an archive."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        if not (self.path.is_dir() or zipfile.is_zipfile(self.path)):
            raise ValueError(f"Expected a directory or ZIP archive: {self.path}")

    @property
    def is_zip(self) -> bool:
        return self.path.is_file()

    def members(self) -> list[Member]:
        if self.is_zip:
            with zipfile.ZipFile(self.path) as archive:
                return [
                    Member(PurePosixPath(info.filename), info.file_size)
                    for info in archive.infolist()
                    if not info.is_dir()
                ]
        return [
            Member(PurePosixPath(file.relative_to(self.path).as_posix()), file.stat().st_size)
            for file in self.path.rglob("*")
            if file.is_file()
        ]

    @contextmanager
    def open_text(self, member: Member) -> Iterator[TextIO]:
        if self.is_zip:
            archive = zipfile.ZipFile(self.path)
            raw = archive.open(member.path.as_posix(), "r")
            import io

            text = io.TextIOWrapper(raw, encoding="utf-8-sig")
            try:
                yield text
            finally:
                text.close()
                archive.close()
        else:
            with (self.path / Path(*member.path.parts)).open(
                "r", encoding="utf-8-sig"
            ) as handle:
                yield handle


def dataset_kind(path: PurePosixPath) -> str | None:
    """Infer A/B from any ancestor named dataset_a or dataset_b."""
    lowered = [part.lower() for part in path.parts]
    for kind in ("dataset_a", "dataset_b"):
        if kind in lowered:
            return kind
    return None


def session_name(path: PurePosixPath) -> str | None:
    return next((part for part in path.parts if part.startswith("ses_")), None)


def chunk_name(path: PurePosixPath) -> str | None:
    return next(
        (part for part in reversed(path.parts) if part.startswith("chunk_")),
        None,
    )