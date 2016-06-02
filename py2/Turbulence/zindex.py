import numpy as np


def morton3d(z, y, x):
    return np.bitwise_or(np.bitwise_or(np.left_shift(partBy2(z), 2), np.left_shift(partBy2(y), 1)), partBy2(x))


def partBy2(x):
    x = np.bitwise_and(x, 0x1f00000000ffff)
    x = np.bitwise_and(np.bitwise_or(x, np.left_shift(x, 32)), 0x1f00000000ffff)
    x = np.bitwise_and(np.bitwise_or(x, np.left_shift(x, 16)), 0x1f0000ff0000ff)
    x = np.bitwise_and(np.bitwise_or(x, np.left_shift(x, 8)), 0x100f00f00f00f00f)
    x = np.bitwise_and(np.bitwise_or(x, np.left_shift(x, 4)), 0x10c30c30c30c30c3)
    x = np.bitwise_and(np.bitwise_or(x, np.left_shift(x, 2)), 0x1249249249249249)
    return x


def mask():
    return np.bitwise_not(8 * 8 * 8 - 1)
