"""engine.learning.multiplicity: family trial count, permutation p-value, registration alpha."""

from __future__ import annotations

import random
from datetime import date

from engine.learning.multiplicity import Trial, family_trials, permutation_pvalue, registration_alpha

POP = [float(i) for i in range(1, 501)]


def _trial(family: str, variants: int) -> Trial:
    return Trial("id", date(2026, 10, 29), family, "h", "p", "c", variants, "null", "r")


def test_family_trials_sums_variants_over_the_family():
    trials = [_trial("regime_filter/hi52", 1), _trial("regime_filter/hi52", 3), _trial("regime_filter/ins", 5)]
    assert family_trials(trials, "regime_filter/hi52") == 4
    assert family_trials(trials, "edge/none") == 0


def test_permutation_pvalue_follows_its_seed():
    sample = POP[200:260]
    assert permutation_pvalue(sample, POP, draws=500, seed=7) != permutation_pvalue(sample, POP, draws=500, seed=8)


def test_permutation_pvalue_counts_ties_and_draws_without_replacement():
    assert permutation_pvalue(POP, POP, draws=200, seed=1) == 1.0


def test_permutation_pvalue_is_about_half_on_random_subsamples():
    rng = random.Random(20261029)
    ps = [permutation_pvalue(rng.sample(POP, 50), POP, draws=1000, seed=k) for k in range(40)]
    assert abs(sum(ps) / len(ps) - 0.5) < 0.12


def test_permutation_pvalue_is_zero_on_the_top_n_subsample():
    assert permutation_pvalue(POP[-30:], POP, seed=1) == 0.0


def test_registration_alpha():
    assert registration_alpha(2) == 0.025
    assert registration_alpha(0) == registration_alpha(1) == 0.05
