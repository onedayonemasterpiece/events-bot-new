"""Pure, deterministic daily layout contract for the service-share cube scene."""
from __future__ import annotations

import hashlib
import random
from datetime import date


FAMILY_ORDER = ("diagonal_ribbon", "ascending_arc", "soft_s_curve")
COMPOSITION_FAMILIES = {
    "diagonal_ribbon": [
        ("HERO", (4.20,-5.35,-1.45), 4.25, "graphite", (0,0,0)),
        ("BRIDGE", (3.25,-1.58,.82), 2.32, "graphite_2", (.02,-.015,-.085)),
        ("A", (2.18,.46,2.34), 1.20, "graphite_2", (-.025,.015,-.065)),
        ("B", (3.50,1.18,2.92), 1.36, "graphite", (.035,-.02,.075)),
        ("C", (4.82,1.90,2.28), 1.25, "graphite_2", (-.03,.02,-.095)),
    ],
    "ascending_arc": [
        ("HERO", (4.12,-5.30,-1.43), 4.28, "graphite", (.012,-.008,-.018)),
        ("BRIDGE", (3.30,-1.62,.78), 2.28, "graphite_2", (.028,-.018,.055)),
        ("A", (2.34,.34,2.38), 1.18, "graphite_2", (-.025,.018,-.075)),
        ("B", (3.34,1.16,3.05), 1.28, "graphite", (.032,-.018,.088)),
        ("C", (4.44,2.06,3.24), 1.08, "graphite_2", (-.026,.018,-.082)),
    ],
    "soft_s_curve": [
        ("HERO", (4.28,-5.30,-1.47), 4.30, "graphite", (-.010,.006,.024)),
        ("BRIDGE", (3.42,-1.66,.72), 2.26, "graphite_2", (.026,-.014,-.058)),
        ("A", (4.06,.30,2.34), 1.22, "graphite", (-.020,.016,.072)),
        ("B", (3.12,1.22,3.05), 1.24, "graphite_2", (.034,-.018,-.082)),
        ("C", (4.05,2.34,3.62), 1.02, "graphite_2", (-.024,.016,.068)),
    ],
}


def resolve_layout(config: dict) -> tuple[str, int, list[tuple]]:
    composition_date = str(config.get("composition_date") or date.today().isoformat())
    requested = str(config.get("composition_family") or "auto")
    if requested == "auto":
        # Ordinal cycling guarantees that adjacent dates use different families.
        family = FAMILY_ORDER[(date.fromisoformat(composition_date).toordinal() + 2) % len(FAMILY_ORDER)]
    elif requested in COMPOSITION_FAMILIES:
        family = requested
    else:
        raise ValueError(f"unknown composition family: {requested}")
    seed_text = str(config.get("composition_seed") or f"service-share-layout-v1|{composition_date}|{family}")
    seed = int.from_bytes(hashlib.sha256(seed_text.encode()).digest()[:8], "big")
    rng = random.Random(seed)
    result = []
    for index, (name, location, size, material, rotation) in enumerate(COMPOSITION_FAMILIES[family]):
        loc_jitter = (.045, .035, .025) if index == 0 else (.10, .09, .08)
        rot_jitter = .007 if index == 0 else .014
        size_jitter = .008 if index == 0 else .018
        varied_location = tuple(value + rng.uniform(-amount, amount) for value, amount in zip(location, loc_jitter))
        varied_rotation = tuple(value + rng.uniform(-rot_jitter, rot_jitter) for value in rotation)
        varied_size = size * (1 + rng.uniform(-size_jitter, size_jitter))
        result.append((name, varied_location, varied_size, material, varied_rotation))
    return family, seed, result
