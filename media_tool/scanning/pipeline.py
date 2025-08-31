import os, hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Tuple, Optional
from PIL import Image
from ..config import DEFAULT_LARGE_FILE_BYTES

import logging
logging.getLogger("PIL.TiffImagePlugin").setLevel(logging.WARNING)

try:
    import imagehash
except Exception:
    imagehash = None

def discover_paths(root: str):
    for dirpath, _, filenames in os.walk(root, followlinks=False):
        for name in filenames:
            full = os.path.join(dirpath, name)
            try:
                st = os.lstat(full)
                # regular file? (POSIX S_IFREG)
                if (st.st_mode & 0o170000) != 0o100000:
                    continue
            except Exception:
                continue
            yield full

def _cap_to_pixels(size: Tuple[int, int], max_pixels: int) -> Tuple[int, int]:
    w, h = size
    if not w or not h: return (w or 0, h or 0)
    if w*h <= max_pixels: return (w, h)
    r = (max_pixels/(w*h))**0.5
    return (max(1, int(w*r)), max(1, int(h*r)))

def _fast_fp(path: str, n: int = 64*1024) -> str:
    h = hashlib.sha1()
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        h.update(f.read(n))
        if size > n:
            f.seek(max(0, size - n))
            h.update(f.read(n))
    return h.hexdigest()

def _extract_features(path: str, max_phash_pixels: int):
    # sha256
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    sha256 = h.hexdigest()
    size = os.path.getsize(path)

    # optional phash
    p_hex = None
    width = height = None
    if imagehash is not None:
        try:
            with Image.open(path) as im:
                im.load()
                width, height = im.size
                im.thumbnail(_cap_to_pixels(im.size, max_phash_pixels))
                ph = imagehash.phash(im)
                p_hex = format(int(str(ph), 16), "016x")
        except Exception:
            pass

    return sha256, p_hex, width, height, size, _fast_fp(path), path

def run_scan_pipeline(
    root: str,
    writer,
    drive_id: int,
    max_phash_pixels: int = 24_000_000,
    io_workers: int = 4,
    cpu_workers: Optional[int] = None,
    filetype: str = "image",
):
    futures = []
    with ProcessPoolExecutor(max_workers=cpu_workers) as pool:
        for path in discover_paths(root):
            futures.append(pool.submit(_extract_features, path, max_phash_pixels))
        for fut in as_completed(futures):
            sha256, p_hex, w, h, size, ffp, path = fut.result()
            row = (
                sha256,            # hash_sha256 (TEXT)
                p_hex,             # phash (TEXT hex or NULL)
                w, h,              # width, height
                size,              # size_bytes
                filetype,          # type
                drive_id,          # drive_id
                path,              # path_on_drive
                int(size >= DEFAULT_LARGE_FILE_BYTES),  # is_large
                0,                 # copied
                None,              # duplicate_of
                None,              # group_id
                None,              # central_path
                ffp,               # fast_fp
            )
            writer.submit(row)
