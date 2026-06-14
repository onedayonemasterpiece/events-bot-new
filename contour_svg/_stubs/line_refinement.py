from __future__ import annotations


def line_optim(lines, *args, **kwargs):
    import numpy as np

    arr = np.asarray(lines)
    if arr.ndim == 2 and arr.shape[1] >= 4:
        out = arr[:, :4].reshape(-1, 2, 2).tolist()
    else:
        out = []
    labels = [-1] * len(out)
    vps = []
    return out, labels, vps
