from chart_pattern_detector import (
    detect_best_chart_pattern,
    detect_double_bottom,
    detect_double_top,
)


def detect_triple_bottom(candles):
    from chart_pattern_detector import detect_triple_bottom as _impl
    return _impl(candles)


def detect_triple_top(candles):
    from chart_pattern_detector import detect_triple_top as _impl
    return _impl(candles)


def detect_best_pattern(candles):
    return detect_best_chart_pattern(candles)
