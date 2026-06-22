from __future__ import annotations

from vntdr.webapp import _auto_fit_parameter_space, _parse_space_value

def test_parse_space_value_discrete() -> None:
    assert _parse_space_value("4,6,8") == [4, 6, 8]
    assert _parse_space_value(" 1, 2.5, 3 ") == [1, 2.5, 3]
    assert _parse_space_value(10) == [10]

def test_parse_space_value_ranges() -> None:
    assert _parse_space_value("4~8") == [4, 5, 6, 7, 8]
    assert _parse_space_value("4-8") == [4, 5, 6, 7, 8]
    assert _parse_space_value("4 to 8") == [4, 5, 6, 7, 8]

def test_parse_space_value_steps() -> None:
    assert _parse_space_value("4~8:2") == [4, 6, 8]
    assert _parse_space_value("4~8 step 2") == [4, 6, 8]
    assert _parse_space_value("4~8/2") == [4, 6, 8]
    assert _parse_space_value("5 to 15 step 3") == [5, 8, 11, 14]

def test_parse_space_value_floats() -> None:
    # Use approximate comparison for floats to avoid floating point accuracy issues
    parsed = _parse_space_value("1.0~1.3:0.1")
    assert len(parsed) == 4
    assert abs(parsed[0] - 1.0) < 1e-9
    assert abs(parsed[1] - 1.1) < 1e-9
    assert abs(parsed[2] - 1.2) < 1e-9
    assert abs(parsed[3] - 1.3) < 1e-9


def test_auto_fit_uses_bounded_recommended_space_not_full_bounds() -> None:
    space = _auto_fit_parameter_space("cm_macd_ult_mtf")

    assert space == {
        "fast_length": [2, 4, 6, 8, 10, 12],
        "slow_length": [10, 15, 20, 25, 30],
        "signal_length": [3, 5, 7, 9],
        "trend_window": [3, 5, 7, 9],
    }
    combinations = 1
    for values in space.values():
        combinations *= len(values)
    assert combinations == 480
