# encoding: utf-8
import numpy as np
import pandas as pd


def quantilize_without_nan(df, n_quantiles=5, axis=-1):
    mat = df.values
    mask = pd.isnull(mat)
    res = mat.copy()

    rank = res.argsort(axis=axis).argsort(axis=axis)
    count = np.sum(~mask, axis=axis)  # int

    divisor = count * 1. / n_quantiles  # float
    shape = list(res.shape)
    shape[axis] = 1
    divisor = divisor.reshape(*shape)
    res = np.floor(rank / divisor) + 1.0

    res[mask] = np.nan

    return res

    # res[~mask] = pd.qcut(mat[~mask], n_quantiles, labels = False) + 1
    # rank = mat.argsort(axis=axis).argsort(axis=axis)  # int
    #
    # count = np.sum(~mask, axis=axis)  # int
    # divisor = count * 1. / n_quantiles  # float
    # shape = list(mat.shape)
    # shape[axis] = 1
    # divisor = divisor.reshape(*shape)
    #
    # res = np.floor(rank / divisor) + 1.0
    # res[mask] = np.nan
    
    #return res


def quantilize_without_nan_another(df, n_quantiles=5, axis=-1):
    '''
    '''
    def quantile_calc(ser):
        mask = pd.isnull(ser)
        ser = pd.qcut(ser[~mask], n_quantiles, labels = False) + 1
        res = pd.Series(index=mask.index, data=ser)
        #res[mask] = np.nan
        return res

    if isinstance(df, pd.DataFrame):
        res = df.apply(quantile_calc, axis=axis)
    elif isinstance(df, pd.Series):
        res = quantile_calc(df)
    else:
        raise ValueError
    return res


# Boolean, unsigned integer, signed integer, float, complex.
_NUMERIC_KINDS = set('buifc')


def is_numeric(array):
    """
    Determine whether the argument has a numeric datatype, when
    converted to a NumPy array.

    Booleans, unsigned integers, signed integers, floats and complex
    numbers are the kinds of numeric datatype.

    Parameters
    ----------
    array : array-like
        The array to check.

    Returns
    -------
    is_numeric : `bool`
        True if the array has a numeric datatype, False if not.

    """
    return np.asarray(array).dtype.kind in _NUMERIC_KINDS
