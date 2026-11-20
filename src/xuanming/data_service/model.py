"""Shared constants, validators and value types of the Python data_service (Go ``shared/accountdirectory``, ``placement``)."""

import re
from dataclasses import dataclass

FIRST_STORAGE_ID = 1_000_000          # placement.FirstStorageID; storage_id >= it is a player store id
ROUTE_HOME_ZONE_MAX = 999_999         # store.RouteHomeZoneMax
ROUTE_SHARDS = 64                     # placement.RouteRowShards; baked into chk_route_shard
ROUTE_BATCH_LIMIT = 1000              # store.RouteBatchLimit
NAME_BATCH_LIMIT = 500                # store.PlayerNameBatchLimit
MAX_ROLE_BYTES = 16384
MAX_ID_STEP, DEFAULT_ID_STEP = 10_000_000, 100   # idissuer.MaxStep / DefaultStep
INITIAL_OWNER_EPOCH = INITIAL_SAVE_SEQ = 1
SCHEMA_GEN = 1                        # shared/generated/persistschema.Gen

# constants/error_codes.go
ERR_INVALID_REQUEST, ERR_NOT_FOUND = 14, 4
ERR_ID_SOURCE, ERR_ID_EXHAUSTED, ERR_ID_UNKNOWN_TAG = 18, 19, 20
ERR_ZONE_CONFLICT, ERR_ADMIN_AUTH, ERR_NAME_DB, ERR_NAME_CONFLICT = 21, 23, 25, 26
ERR_ROUTE_STORE, ERR_NAME_IN_USE = 30, 38

# config.DefaultIdSegmentTags (data_service.yaml IdSegment.Tags)
DEFAULT_ID_TAGS = (
    "player", "guild", "item", "txlog", "snapshot", "trade_listing", "guild_asset_op", "friend_request",
    "trade_asset_op", "mail", "mail_asset_op", "rank_board", "rank_settlement", "rank_reward", "battle",
    "snapshot_row", "rollback_audit", "gm_recall_op",
)

_TAG = re.compile(r"[a-z0-9_]{1,64}")
_INSTANCE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


@dataclass(frozen=True, slots=True)
class Ownership:
    account: str
    role: bytes  # serialized AccountSimplePlayer
    owner_revision: int
    trade_listing_id: int = 0
    prepare_token: int = 0
    initial_player_commit: bool = False


def valid_account(account: str) -> bool:
    try:
        return 0 < len(account.encode()) <= 191
    except UnicodeEncodeError:
        return False


def valid_tag(tag: str) -> bool:
    return _TAG.fullmatch(tag) is not None


def valid_instance(instance: str) -> bool:
    return _INSTANCE.fullmatch(instance) is not None


def valid_save_floor(epoch: int, seq: int) -> bool:
    return (epoch == 0) == (seq == 0)


def _valid_node(node: str) -> bool:
    return 0 < len(node.encode()) <= 96 and not any(char in node for char in "\x00\r\n\t ")


def valid_admission(identity) -> bool:
    i = identity
    return (i.player_id > 0 and valid_instance(i.scene_instance_id) and i.generation > 0 and i.owner_revision > 0
            and i.owner_epoch > 0 and _valid_node(i.scene_node_id) and i.scene_id > 0 and i.zone_id > 0
            and i.session_id > 0 and i.login_generation > 0)


class MergedZones:
    """Snapshot of ``zone_merged_into`` (src -> dst); ``final`` follows merge chains (placement.MergedZones)."""

    def __init__(self, mapping: dict[int, int]) -> None:
        self.mapping = mapping

    def final(self, zone: int) -> int:
        for _ in range(len(self.mapping) + 1):  # the bound also stops a corrupt cycle
            if zone not in self.mapping:
                return zone
            zone = self.mapping[zone]
        return zone
