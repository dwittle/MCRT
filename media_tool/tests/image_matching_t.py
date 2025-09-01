import os
import pytest
from PIL import Image
import imagehash
from mcrt import FeatureExtractor, DuplicateDetector, FileRecord, DatabaseManager
from pathlib import Path

GENERATED_IMG_DIR = Path(__file__).parent / "generated"
DB_PATH = Path(__file__).parent / "test_db.sqlite"

def get_generated_pairs():
    # Find all files and match those with the same base filename (up to first '_')
    files = [f for f in GENERATED_IMG_DIR.iterdir() if f.is_file()]
    pairs = []
    # Group files by base name
    groups = {}
    for file in files:
        base = file.name.split('_')[0]
        groups.setdefault(base, []).append(file)
    # Create all unique pairs within each group
    for group_files in groups.values():
        for i in range(len(group_files)):
            for j in range(i+1, len(group_files)):
                f1 = group_files[i].name
                f2 = group_files[j].name
                # Skip testing 'fail' images against images with the same prefix
                if "fail" in f1 or "fail" in f2:
                    # If both are 'fail', allow test; if only one is 'fail', skip
                    if ("fail" in f1 and "fail" not in f2) or ("fail" in f2 and "fail" not in f1):
                        continue
                expected = not ("fail" in f1 or "fail" in f2)
                pairs.append((f1, f2, expected))
    return pairs

@pytest.fixture(scope="session")
def db_manager():
    # Use a temporary DB for testing
    if DB_PATH.exists():
        DB_PATH.unlink()
    return DatabaseManager(DB_PATH)

@pytest.mark.parametrize("img_name1,img_name2,expected", get_generated_pairs())
def test_phash_matching_generated(img_name1, img_name2, expected, db_manager):
    extractor = FeatureExtractor()
    img1_path = GENERATED_IMG_DIR / img_name1
    img2_path = GENERATED_IMG_DIR / img_name2
    rec1 = extractor.extract_features(img1_path, img1_path.stat().st_size, False, set())
    rec2 = extractor.extract_features(img2_path, img2_path.stat().st_size, False, set())
    assert rec1.phash is not None and rec2.phash is not None
    detector = DuplicateDetector(db_manager)
    detector._phash_groups[rec1.phash] = {1}
    group = detector._find_similar_phash_group(rec2.phash, threshold=5)
    dist = imagehash.hex_to_hash(rec1.phash) - imagehash.hex_to_hash(rec2.phash)
    print(f"{img_name1} vs {img_name2}: phash1={rec1.phash}, phash2={rec2.phash}, distance={dist}")
    assert (group is not None) == expected
