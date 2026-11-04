"""Typical residential and commercial water use, for synthetic meters.

Each meter type has a daily volume and a shape across the day. The volumes
come from published figures; the shapes and spreads are assumptions, marked
as such, until a published diurnal curve for the region is found.

Sourced:
* Residential: 169 L per person per day, Melbourne 2024/25 (Greater Western
  Water, Melbourne's Annual Water Outlook 2026), and 2.6 people per household
  in Greater Melbourne (ABS 2021 Census QuickStats).
* Commercial: average L per m² of floor area per day by use, Sydney Water's
  Non-residential water usage guideline (SW 66 03/25, version 3, March 2025).
  No Melbourne equivalent was found, so the Sydney figures stand in.
* Day-to-day variation: residential peak-day to average-day ratios of 1 to
  1.5 (Beal and Stewart, J. Water Resour. Plann. Manage. 140(7), 2014); the
  daily spread below stays inside that range.

Assumed (change these when better data is available):
* Diurnal and weekend shapes, the spread of household and floor sizes, the
  dwellings behind a bulk meter, the day and interval ranges, and how often a
  household meter reads zero in a quiet interval.

A meter's size is fixed by its id, as a real household or shop stays the
same size from one reading to the next. Readings are random within a range:
each interval is drawn afresh, uniformly within METER_INTERVAL_RANGE of its
expected volume, and each day uniformly within METER_DAY_RANGE of the meter's
average day. A day's factor holds for that day while the service runs and is
drawn again after a restart.

Standard library only, so the numbers can be tested without Kafka or a
database; tests pass a seeded generator for repeatable draws.

    meter_profile(meter_id, meter_type) -> Profile
    interval_litres(profile, start, minutes, rng=None) -> float
"""

from __future__ import annotations

import hashlib
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime

# --- sourced figures --------------------------------------------------------
LITRES_PER_PERSON_DAY = 169.0          # Melbourne 2024/25
PEOPLE_PER_HOUSEHOLD = 2.6             # Greater Melbourne, 2021 Census
COMMERCIAL_L_PER_M2_DAY = {            # Sydney Water, March 2025
    "office": 0.4,
    "retail": 1.5,
    "shopping_centre": 2.0,
    "cafe": 2.5,
    "medical": 1.5,
}
PEAK_DAY_RATIO_MAX = 1.5               # upper end of the indoor-driven range

# --- assumptions -------------------------------------------------------------
# Hourly multipliers, normalised below to average 1 over the day. Residential:
# a morning peak around 7-8 am and a broader evening peak, little overnight.
_RESIDENTIAL_WEEKDAY = [0.35, 0.25, 0.20, 0.20, 0.25, 0.50, 1.30, 1.90, 1.70, 1.30, 1.10, 1.00,
                        0.95, 0.90, 0.85, 0.90, 1.05, 1.35, 1.60, 1.55, 1.30, 1.00, 0.70, 0.50]
# Weekends start later and spread the morning peak.
_RESIDENTIAL_WEEKEND = [0.40, 0.30, 0.25, 0.20, 0.20, 0.30, 0.60, 1.10, 1.60, 1.80, 1.60, 1.30,
                        1.20, 1.10, 1.00, 1.00, 1.10, 1.30, 1.50, 1.45, 1.20, 0.95, 0.70, 0.50]
# Commercial: trading hours, with a small base overnight (cooling, cisterns).
_COMMERCIAL = [0.10, 0.10, 0.10, 0.10, 0.10, 0.15, 0.40, 1.00, 1.80, 2.00, 2.00, 2.00,
               2.10, 2.00, 1.90, 1.80, 1.60, 1.00, 0.50, 0.30, 0.20, 0.15, 0.10, 0.10]
COMMERCIAL_WEEKEND_FACTOR = 0.4        # weekend volume against a weekday
HOUSEHOLD_SPREAD = 0.35                # lognormal sigma on household volume
FLOOR_AREA_MEDIAN_M2 = 300.0
FLOOR_AREA_SPREAD = 0.8                # lognormal sigma on floor area
FLOOR_AREA_RANGE_M2 = (50.0, 5000.0)
BULK_DWELLINGS = (10, 60)              # dwellings behind one bulk meter
# Uniform ranges either side of the expected value. The day range stays well
# inside the published peak-day ratio of 1.5.
DAY_RANGE = float(os.environ.get("METER_DAY_RANGE", "0.15"))
INTERVAL_RANGE = float(os.environ.get("METER_INTERVAL_RANGE", "0.5"))
if not (0 <= DAY_RANGE < 1 and 0 <= INTERVAL_RANGE < 1):
    raise SystemExit("! METER_DAY_RANGE and METER_INTERVAL_RANGE must be 0 to below 1")
# Salts the day factor, so it is steady within a day but differs between runs.
_RUN_SALT = random.SystemRandom().getrandbits(64)
_DEFAULT_RNG = random.Random()
# Below this hourly multiplier a single household or shop often uses nothing
# in an interval; the chance of a zero rises as the multiplier falls. A bulk
# meter aggregates many dwellings, so it never drops to zero.
QUIET_MULTIPLIER = 0.8


def _normalised(values):
    mean = sum(values) / len(values)
    return [v / mean for v in values]


RESIDENTIAL_WEEKDAY = _normalised(_RESIDENTIAL_WEEKDAY)
RESIDENTIAL_WEEKEND = _normalised(_RESIDENTIAL_WEEKEND)
COMMERCIAL = _normalised(_COMMERCIAL)


@dataclass(frozen=True)
class Profile:
    meter_id: str
    meter_type: str          # residential | commercial | bulk
    daily_litres: float      # average weekday volume
    detail: str              # what the volume was drawn from, for logs


def _rng(*parts) -> random.Random:
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _lognormal(rng: random.Random, median: float, sigma: float) -> float:
    return median * math.exp(rng.gauss(0.0, sigma))


def meter_profile(meter_id: str, meter_type: str) -> Profile:
    """The meter's size, fixed by its id."""
    rng = _rng("profile", meter_id)
    household = LITRES_PER_PERSON_DAY * PEOPLE_PER_HOUSEHOLD
    # A lognormal's mean is its median times exp(sigma²/2); dividing that out
    # keeps the population mean on the sourced figure.
    unbias = math.exp(HOUSEHOLD_SPREAD ** 2 / 2)
    if meter_type == "commercial":
        use = rng.choice(sorted(COMMERCIAL_L_PER_M2_DAY))
        lo, hi = FLOOR_AREA_RANGE_M2
        area = min(hi, max(lo, _lognormal(rng, FLOOR_AREA_MEDIAN_M2, FLOOR_AREA_SPREAD)))
        return Profile(meter_id, meter_type, area * COMMERCIAL_L_PER_M2_DAY[use],
                       f"{use}, {area:.0f} m²")
    if meter_type == "bulk":
        dwellings = rng.randint(*BULK_DWELLINGS)
        return Profile(meter_id, meter_type, dwellings * household, f"{dwellings} dwellings")
    # Anything else is read as a household, the commonest connection.
    volume = _lognormal(rng, household / unbias, HOUSEHOLD_SPREAD)
    return Profile(meter_id, "residential", volume, "household")


def hourly_multiplier(meter_type: str, when: datetime) -> float:
    weekend = when.weekday() >= 5
    if meter_type == "commercial":
        return COMMERCIAL[when.hour] * (COMMERCIAL_WEEKEND_FACTOR if weekend else 1.0)
    return (RESIDENTIAL_WEEKEND if weekend else RESIDENTIAL_WEEKDAY)[when.hour]


def day_factor(meter_id: str, when: datetime, salt: int = _RUN_SALT) -> float:
    """This meter's volume on this date against its average day: uniform
    within DAY_RANGE, and the same for every interval of the day."""
    rng = _rng("day", salt, meter_id, when.date().isoformat())
    return min(PEAK_DAY_RATIO_MAX, rng.uniform(1.0 - DAY_RANGE, 1.0 + DAY_RANGE))


def interval_litres(profile: Profile, start: datetime, minutes: int,
                    rng: random.Random | None = None) -> float:
    """Litres through the meter in the interval starting at `start`, drawn
    afresh each call within INTERVAL_RANGE of the expected volume."""
    rng = rng or _DEFAULT_RNG
    m = hourly_multiplier(profile.meter_type, start)
    mean = profile.daily_litres * day_factor(profile.meter_id, start) * m * minutes / 1440.0
    if profile.meter_type != "bulk" and m < QUIET_MULTIPLIER:
        p_zero = 1.0 - m / QUIET_MULTIPLIER
        if rng.random() < p_zero:
            return 0.0
        mean /= 1.0 - p_zero          # the intervals that do draw carry the mean
    return mean * rng.uniform(1.0 - INTERVAL_RANGE, 1.0 + INTERVAL_RANGE)
