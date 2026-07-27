"""Intelligence-domain event payloads published on the bus (§3.2.1 canonical topics).

The budget governor is the only originator here: a degrade-tier change drives the dashboard state, the
Telegram alert (R8), and the D6 mode-capability mapping consumers apply. Publishing it as data on
``budget.state`` keeps ``risk``/``oms`` free of any import of this package (R1, §2.3).
"""

from __future__ import annotations

from pydantic import AwareDatetime, BaseModel

from engine.core.enums import DegradeTier
from engine.intelligence.schemas import DecimalStr

TOPIC_BUDGET_STATE = "budget.state"


class BudgetStateChanged(BaseModel):
    old_tier: DegradeTier
    new_tier: DegradeTier
    month_spend_usd: DecimalStr
    at: AwareDatetime
