#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from PIL import Image
import shutil

def process_images(orig_dir, gen_dir):
    os.makedirs(gen_dir, exist_ok=True)
    files = [f for f in os.listdir(orig_dir) if Path(orig_dir, f).is_file()]
    # Standard generation
    for fname in files:
        orig_path = Path(orig_dir) / fname
        stem, ext = os.path.splitext(fname)
        copy_name = f"{stem}_copy{ext}"
        shutil.copy2(orig_path, Path(gen_dir) / copy_name)
        try:
            img = Image.open(orig_path)
            for size in [(80, 80), (120, 120), (640, 480), (1024, 780)]:
                resized = img.resize(size)
                resized.save(Path(gen_dir) / f"{stem}_{size[0]}x{size[1]}{ext}")
        except Exception as e:
            print(f"Skipping {fname}: {e}")
    # Negative test: copy and rename 2 files with the prefix from another file, add '_fail'
    if len(files) >= 3:
        # Use the stem of the third file for negative test prefix
        fail_prefix = os.path.splitext(files[2])[0]
        # Pick two other files
        for fail_idx in [1, 2]:
            fail_src = Path(orig_dir) / files[fail_idx]
            _, fail_ext = os.path.splitext(files[fail_idx])
            fail_name = f"{fail_prefix}_fail{fail_idx}{fail_ext}"
            shutil.copy2(fail_src, Path(gen_dir) / fail_name)

if __name__ == "__main__":
    orig_dir = "tests/originals"
    gen_dir = "tests/generated"
    process_images(orig_dir, gen_dir)
