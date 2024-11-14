import ee


def get_reducers(names: list) -> ee.Reducer:
    """Get a reducer that combines all the individual Reducers"""

    reducer = reducer_factory(names[0])
    for feature in names[1:]:
        reducer = reducer.combine(reducer2=reducer_factory(feature), sharedInputs=True)
    return reducer


def reducer_factory(name: str) -> ee.Reducer:
    """Get a ee.Reducer by name"""

    if name == "mean":
        return ee.Reducer.mean()
    elif name == "stdDev":
        return ee.Reducer.stdDev()
    elif name == "median":
        return ee.Reducer.median()
    elif name == "min":
        return ee.Reducer.min()
    elif name == "max":
        return ee.Reducer.max()
    elif name == "skew":
        return ee.Reducer.skew()
    elif name == "kurtosis":
        return ee.Reducer.kurtosis()
    else:
        raise ValueError(f"Unknown reducer {name}")
