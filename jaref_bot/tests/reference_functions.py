import numpy as np

def reference_window_linreg(x, y, window):
    """Простой эталон: для каждого окна решаем обычную OLS через numpy.linalg.lstsq.
    Возвращаем массив shape=(n-window+1, 2) с (slope, intercept).
    Это независимая, простая и надежная реализация.
    """
    n = len(y)
    coefs = np.empty((n - window + 1, 2), dtype=float)
    for i in range(n - window + 1):
        xi = x[i:i+window]
        yi = y[i:i+window]
        A = np.vstack([xi, np.ones_like(xi)]).T
        slope, intercept = np.linalg.lstsq(A, yi, rcond=None)[0]
        coefs[i, 0] = slope
        coefs[i, 1] = intercept
    return coefs
