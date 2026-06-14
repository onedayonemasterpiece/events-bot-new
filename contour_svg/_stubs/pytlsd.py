from __future__ import annotations


def lsd(image, *, scale=1.0, gradnorm=None, gradangle=None, grad_nfa=True):
    import numpy as np
    import cv2

    arr = np.asarray(image)
    if arr.dtype != np.uint8:
        arr = np.clip(arr, 0, 255).astype("uint8")
    detector = cv2.createLineSegmentDetector(0)
    detected = detector.detect(arr)[0]
    if detected is None:
        return np.zeros((0, 5), dtype=np.float32)
    rows = []
    for item in detected:
        x1, y1, x2, y2 = [float(v) for v in item[0]]
        rows.append([x1, y1, x2, y2, 1.0])
    return np.asarray(rows, dtype=np.float32)
