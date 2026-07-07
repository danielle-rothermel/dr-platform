"""Physical naming configuration.

Two constraints force this module (design: platform.md): the library
owns its tables and migrations, and adopters with pre-existing data
(whetstone) have frozen physical names — table prefix ``dr_dspy`` and
domain column words (``prediction_id`` / ``fair_order_key`` /
``experiment_name``). ``PlatformNaming`` parameterizes exactly those
names; everything else (neutral column names, status values, digest
payload shapes) is identical for every adopter.

Constraint/index names are library-generated and NOT parameterized
beyond the prefix: for stamped-baseline adopters the physical objects
already exist under their historical names and revision 0001 never
runs, so only table/column names must match.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr

from dr_platform.items import ItemIdentity

DEFAULT_PREFIX = "dr_platform"


class PlatformNaming(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    prefix: StrictStr = DEFAULT_PREFIX
    item_key_label: StrictStr = "item_id"
    order_key_label: StrictStr = "order_key"
    group_key_label: StrictStr = "group_key"
    id_length: StrictInt = 32

    @property
    def identity(self) -> ItemIdentity:
        return ItemIdentity(
            item_key_label=self.item_key_label,
            id_length=self.id_length,
        )

    def table_name(self, base: str) -> str:
        return f"{self.prefix}_{base}"

    @property
    def batch_operations_table(self) -> str:
        return self.table_name("batch_submit_operations")

    @property
    def batch_items_table(self) -> str:
        return self.table_name("batch_submit_items")

    @property
    def throttle_backoff_table(self) -> str:
        return self.table_name("throttle_backoff")

    @property
    def projections_table(self) -> str:
        return self.table_name("projections")

    @property
    def alembic_version_table(self) -> str:
        return self.table_name("platform_alembic_version")
