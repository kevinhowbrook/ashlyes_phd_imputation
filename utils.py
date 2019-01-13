from math import isclose, sqrt

def error_gen(actual, rounded):
    divisor = sqrt(1.0 if actual < 1.0 else actual)
    return abs(rounded - actual) ** 2 / divisor

def round_to_100(percents):
    if not isclose(sum(percents), 100):
        raise ValueError
    n = len(percents)
    rounded = [int(x) for x in percents]
    up_count = 100 - sum(rounded)
    errors = [(error_gen(percents[i], rounded[i] + 1) - error_gen(percents[i], rounded[i]), i) for i in range(n)]
    rank = sorted(errors)
    for i in range(up_count):
        rounded[rank[i][1]] += 1
    return rounded


def round_percentages(percentages):
    # list will be like [[1.0, 54.5], [2.0, 40.9]... etc]
    # so pick out the numbers to round
    list_to_round = []
    for p in percentages:
        list_to_round.append(p[1])

    rounded = round_to_100(list_to_round)
    # make a new list to return
    _return = []
    for i,p in enumerate(percentages):
        _return.append([p[0], rounded[i]])

    return _return

#rounded = round_percentages([[1.0, 54.54545454545454], [3.0, 40.909090909090914], [4.0, 4.545454545454546]])
