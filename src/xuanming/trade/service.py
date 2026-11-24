"""trade 三组 gRPC 服务适配层与 fail-closed 鉴权。

对外暴露：
- ``trade.ClientPlayerJubaozhai``: P1 目录 (4) + 核心交易 (18) + 官方商城/人民币 (13)
- ``trade.TradeAdmin``: SeedListing / ListManualContracts / ResolveManualContract
- ``rolefence.RoleFenceParticipant``: PrepareRoleFreeze / ApplyRoleFence / PurgeRemovedRole
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from collections.abc import Awaitable, Callable

import grpc
from google.protobuf.message import Message

from ..grpc_runtime import verify_service_call
from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol
from .commerce_engine import (
    CommerceEngine,
    ManualRequest,
    decode_workflow,
    owns_record,
    parse_partial_part,
)
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_SERVICE_UNAVAILABLE,
    ERR_TRADE_ASSET_DELIVERY_PENDING,
    ERR_TRADE_COUNTERPARTY_UNKNOWN,
    ERR_TRADE_FAVORITE_LIMIT_REACHED,
    ERR_TRADE_FEATURE_DISABLED,
    ERR_TRADE_HOME_ZONE_UNKNOWN,
    ERR_TRADE_LISTING_NOT_FOUND,
    ERR_TRADE_ORDER_NOT_FOUND,
    InvalidRoleFenceStateError,
    ListingNotFoundError,
    ManualNotAllowedError,
    ManualNotFoundError,
    ManualVersionChangedError,
    TradeRejection,
    TradeRoleConflictError,
    commerce_tip_id,
)
from .model import (
    COMMERCE_KIND_AUCTION_LOT,
    COMMERCE_KIND_BID,
    COMMERCE_KIND_CONSIGNMENT,
    COMMERCE_KIND_OFFICIAL,
    LISTING_SECTION_AUCTION,
    LISTING_STATUS_LISTED,
    MANUAL_LIST_PAGE_MAX,
    MANUAL_RESOLUTION_CALLER,
    MARKET_SCOPE_GLOBAL,
    MARKET_SCOPE_ZONE,
    CommerceCursor,
    ListingQuery,
    MarketLimits,
    MergedZones,
    RoleOriginal,
    clamp_page_size,
    clamp_uint32,
    escape_like,
    normalize_search,
    page_window,
    to_listing_summary,
    valid_account,
    valid_category,
    valid_manual_environment,
    valid_section,
    valid_seed_request,
    valid_sort,
    valid_tab,
    visible_to_buyer,
)
from .store import HomeZoneDirectory, TradeStoreBase

logger = logging.getLogger(__name__)

SERVICE_CLIENT = "trade.ClientPlayerJubaozhai"
SERVICE_ADMIN = "trade.TradeAdmin"
SERVICE_ROLE_FENCE = "rolefence.RoleFenceParticipant"

CODE = grpc.StatusCode

# ManualContractMode / ManualContractVerdict 枚举值
_MANUAL_MODE_SHOW = 1
_MANUAL_MODE_DRY_RUN = 2
_MANUAL_MODE_EXECUTE = 3

_VERDICT_OK = 1
_VERDICT_NOT_FOUND = 2
_VERDICT_REFUSED = 3
_VERDICT_VERSION_CHANGED = 4
_VERDICT_RETRY = 5


def _has_session_metadata(context) -> bool:
    if context is None:
        return False
    for key, _ in context.invocation_metadata() or ():
        if key.lower() == SESSION_DETAIL:
            return True
    return False


class TradeService:
    """集中挂载 ClientPlayerJubaozhai / TradeAdmin / RoleFenceParticipant 三个 gRPC 服务。"""

    def __init__(
        self,
        protocol: Protocol,
        store: TradeStoreBase,
        *,
        caller_verifier: CallerVerifier | None,
        coordinator_secret: bytes,
        home_zones: HomeZoneDirectory | None = None,
        listing_ids: Callable[[], Awaitable[int]] | None = None,
        commerce: CommerceEngine | None = None,
        market: MarketLimits = MarketLimits(),
        zones_provider: Callable[[], MergedZones] | None = None,
        mode: str = "pro",
        mall_url: str = "",
        rmb_enabled: bool = False,
        manual_resolution_secret: bytes = b"",
        manual_resolution_environment: str = "",
        request_budget_sec: float = 3.5,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self._caller_verifier = caller_verifier
        self._coordinator_secret = coordinator_secret
        self._home_zones = home_zones
        self._listing_ids = listing_ids
        self.commerce = commerce
        self._market = market
        self._zones_provider = zones_provider or (lambda: MergedZones())
        self._mode = mode
        self._mall_url = mall_url
        self._rmb_enabled = rmb_enabled
        self._manual_secret = manual_resolution_secret
        self._manual_target = manual_resolution_environment
        self._replica = socket.gethostname() or "unknown"
        self._budget_sec = request_budget_sec
        self._clock_ms = clock_ms

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            SERVICE_CLIENT: {
                # P1 Catalog (4)
                "BrowseListings": self.browse_listings,
                "GetListingDetail": self.get_listing_detail,
                "SetFavorite": self.set_favorite,
                "GetMyShelf": self.get_my_shelf,
                # Official Mall (2)
                "ListOfficialSkus": self.list_official_skus,
                "CreateOfficialOrder": self.create_official_order,
                # Core Commerce (18)
                "GetCommerceOrder": self.get_commerce_order,
                "CancelCommerceOrder": self.cancel_commerce_order,
                "ListCommerceOrders": self.list_commerce_orders,
                "CreatePlayerTrade": self.create_player_trade,
                "ConfirmPlayerTrade": self.confirm_player_trade,
                "CreateConsignment": self.create_consignment,
                "BuyConsignment": self.buy_consignment,
                "CancelConsignment": self.cancel_consignment,
                "PlaceMarketOrder": self.place_market_order,
                "CancelMarketOrder": self.cancel_market_order,
                "ListMarketOrders": self.list_market_orders,
                "GetMarketDepth": self.get_market_depth,
                "CreateAuctionLot": self.create_auction_lot,
                "PlaceAuctionBid": self.place_auction_bid,
                "GetAuctionLot": self.get_auction_lot,
                "CancelAuctionLot": self.cancel_auction_lot,
                "BrowseCommerceOffers": self.browse_commerce_offers,
                "ListTradeMarkets": self.list_trade_markets,
                # RMB (11)
                "CreateRmbConsignment": self.create_rmb_consignment,
                "GetRmbConsignment": self.get_rmb_consignment,
                "ListRmbConsignments": self.list_rmb_consignments,
                "BuyRmbConsignment": self.buy_rmb_consignment,
                "CancelRmbConsignment": self.cancel_rmb_consignment,
                "GetRmbOrder": self.get_rmb_order,
                "ListRmbOrders": self.list_rmb_orders,
                "CancelRmbOrder": self.cancel_rmb_order,
                "ListSellerIncomes": self.list_seller_incomes,
                "CreateWithdrawal": self.create_withdrawal,
                "GetWithdrawal": self.get_withdrawal,
            },
            SERVICE_ADMIN: {
                "SeedListing": self.seed_listing,
                "ListManualContracts": self.list_manual_contracts,
                "ResolveManualContract": self.resolve_manual_contract,
            },
            SERVICE_ROLE_FENCE: {
                "PrepareRoleFreeze": self.prepare_role_freeze,
                "ApplyRoleFence": self.apply_role_fence,
                "PurgeRemovedRole": self.purge_removed_role,
            },
        }

    # -- 鉴权与拦截门禁（fail-closed） ------------------------------------------------------------------

    async def _admit_client(self, context, path: str) -> RoleOriginal:
        if not _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "client trade RPC requires verified gate session")
        if self._caller_verifier is None:
            await context.abort(CODE.UNAVAILABLE, "gate caller verifier not configured")
        try:
            raw = self._caller_verifier.authenticate(context.invocation_metadata(), path)
            if raw is None:
                await context.abort(CODE.PERMISSION_DENIED, "missing gate session identity")
            details = parse_details(self.protocol, raw)
        except AuthError as exc:
            await context.abort(CODE.UNAUTHENTICATED, f"invalid gate session signature: {exc}")
        player_id = int(details.player_id)
        owner_rev = int(details.owner_revision)
        account = str(details.authenticated_account)
        if not player_id or not owner_rev or not valid_account(account):
            await context.abort(CODE.PERMISSION_DENIED, "incomplete online role identity in gate session")
        original = RoleOriginal(player_id=player_id, account=account, owner_revision=owner_rev)
        try:
            await self.store.verify_original_owner(original)
        except TradeRoleConflictError as exc:
            await context.abort(CODE.PERMISSION_DENIED, f"role ownership rejected: {exc}")
        except Exception as exc:
            logger.warning("trade ownership precheck inconclusive player=%d err=%s", player_id, exc)
            await context.abort(CODE.ABORTED, "role ownership check inconclusive")
        return original

    async def _reject_client_session(self, context, service_name: str) -> None:
        if _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, f"client session cannot invoke {service_name}")

    async def _admit_coordinator(self, context, request: Message, path: str) -> None:
        await self._reject_client_session(context, SERVICE_ROLE_FENCE)
        if len(self._coordinator_secret) < 32:
            await context.abort(CODE.UNAVAILABLE, "role fence coordinator secret not configured")
        if not verify_service_call(path, request, "coordinator", self._clock_ms(), self._coordinator_secret):
            await context.abort(CODE.PERMISSION_DENIED, "invalid role fence coordinator signature")

    async def _admit_manual_ops(self, context, request: Message, path: str) -> None:
        await self._reject_client_session(context, SERVICE_ADMIN)
        if (
            self.commerce is None
            or len(self._manual_secret) < 32
            or not valid_manual_environment(self._manual_target)
        ):
            logger.error("trade manual resolution entry disabled path=%s", path)
            await context.abort(CODE.PERMISSION_DENIED, "manual resolution entry is disabled")
        if not verify_service_call(path, request, MANUAL_RESOLUTION_CALLER, self._clock_ms(), self._manual_secret):
            logger.error("trade manual resolution caller proof invalid path=%s", path)
            await context.abort(CODE.PERMISSION_DENIED, "manual resolution caller proof is invalid")

    async def _with_budget(self, coro):
        if self._budget_sec > 0:
            async with asyncio.timeout(self._budget_sec):
                return await coro
        return await coro

    def _now_ms(self) -> int:
        return max(0, int(self._clock_ms()))

    async def _resolve_home_zone(self, player_id: int, method: str) -> tuple[int, int]:
        if self._home_zones is None:
            logger.error("trade %s: home_zone lookup not wired player=%d", method, player_id)
            return 0, ERR_SERVICE_UNAVAILABLE
        try:
            zone = await self._home_zones.home_zone(player_id)
        except Exception as exc:
            logger.error("trade %s: home_zone lookup failed player=%d err=%s", method, player_id, exc)
            return 0, ERR_SERVICE_UNAVAILABLE
        if zone == 0:
            return 0, ERR_TRADE_HOME_ZONE_UNKNOWN
        return int(zone), 0

    # -- 视图投影辅助 -------------------------------------------------------------------------------

    def _order_view(self, rec: Message | None) -> Message | None:
        if rec is None:
            return None
        w = None
        try:
            w = decode_workflow(self.protocol, rec)
        except Exception:
            pass
        kind = COMMERCE_KIND_AUCTION_LOT if int(rec.kind) == COMMERCE_KIND_BID else int(rec.kind)
        view = self.protocol.message(
            "trade.CommerceOrderView",
            order_id=str(rec.commerce_id),
            kind=kind,
            state=int(rec.status),
            seller_player_id=int(rec.seller_player_id),
            buyer_player_id=int(rec.buyer_player_id),
            amount=int(rec.unit_price),
            currency=str(rec.currency_type),
            created_ms=int(rec.created_ms),
            version=int(rec.version),
            related_id=str(rec.parent_id),
        )
        if w is not None:
            view.seller_confirmed = bool(w.seller_confirmed)
            view.buyer_confirmed = bool(w.buyer_confirmed)
            if int(rec.kind) == COMMERCE_KIND_OFFICIAL:
                view.order_id = str(w.external_order_sn)
                view.related_id = str(w.sku_id)
                view.amount = int(w.external_amount)
                view.currency = str(w.external_currency)
        return view

    def _order_response(self, rec: Message | None, error: BaseException | None = None, tip: int = 0) -> Message:
        now = self._now_ms()
        resp = self.protocol.message("trade.CommerceOrderResponse", server_now_ms=now)
        code = tip if tip != 0 else commerce_tip_id(error)
        if code != 0:
            resp.error_message.id = code
            return resp
        if rec is None:
            return resp
        try:
            w = decode_workflow(self.protocol, rec)
        except Exception:
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        resp.order.CopyFrom(self._order_view(rec))
        if w.HasField("seller_assets"):
            resp.seller_assets.CopyFrom(w.seller_assets)
        if w.HasField("buyer_assets"):
            resp.buyer_assets.CopyFrom(w.buyer_assets)
        titles = self.commerce.policy.item_titles if self.commerce is not None else {}
        for l in w.legs:
            if int(l.outcome) != 2 or not l.HasField("snapshot"):
                continue
            target_items = resp.seller_item_instances if l.role == "seller_debit" else (
                resp.buyer_item_instances if l.role == "buyer_debit" else None
            )
            if target_items is None:
                continue
            for it in l.snapshot.items:
                entry = target_items.add()
                entry.instance_id = int(it.item_uuid)
                entry.config_id = int(it.config_id)
                entry.count = int(it.stack_size)
                entry.name = titles.get(int(it.config_id), "")
            if l.snapshot.HasField("pet") and int(l.snapshot.pet.pet_id) != 0:
                pet_target = resp.seller_pet if l.role == "seller_debit" else resp.buyer_pet
                pet_target.instance_id = int(l.snapshot.pet.pet_id)
                pet_target.config_id = int(l.snapshot.pet.pet_table_id)
                pet_target.name = str(l.snapshot.pet.name)
                pet_target.level = int(l.snapshot.pet.level)
        for sel, inst_list in (
            (resp.seller_assets if resp.HasField("seller_assets") else None, resp.seller_item_instances),
            (resp.buyer_assets if resp.HasField("buyer_assets") else None, resp.buyer_item_instances),
        ):
            if sel is not None:
                for cfg_id, cnt in sel.items.items():
                    entry = inst_list.add()
                    entry.config_id = int(cfg_id)
                    entry.count = int(cnt)
                    entry.name = titles.get(int(cfg_id), "")
            for inst in inst_list:
                if not inst.name:
                    inst.name = titles.get(int(inst.config_id), "")
        return resp

    def _market_view(self, rec: Message | None) -> Message | None:
        if rec is None:
            return None
        stage = ""
        try:
            stage = decode_workflow(self.protocol, rec).stage
        except Exception:
            pass
        return self.protocol.message(
            "trade.MarketOrderView",
            market_order_id=str(rec.commerce_id),
            market_id=int(rec.market_id),
            item_config_id=int(rec.item_config_id),
            side=int(rec.side),
            quantity=int(rec.quantity),
            filled_quantity=int(rec.filled_quantity),
            unit_price=int(rec.unit_price),
            currency_type=int(rec.currency_type),
            state=stage,
            created_ms=int(rec.created_ms),
        )

    def _lot_view(self, rec: Message | None) -> Message | None:
        if rec is None:
            return None
        try:
            w = decode_workflow(self.protocol, rec)
        except Exception:
            return None
        return self.protocol.message(
            "trade.AuctionLotView",
            lot_id=str(rec.commerce_id),
            seller_player_id=int(rec.seller_player_id),
            currency_type=int(rec.currency_type),
            opening_price=int(rec.unit_price),
            minimum_increment=int(w.minimum_increment),
            highest_bid=int(w.highest_bid),
            highest_bidder=int(w.highest_bidder),
            end_ms=int(rec.expires_ms),
            state=w.stage,
            version=int(rec.version),
            order_id=str(rec.commerce_id),
        )

    # -- P1 Catalog (4 RPCs) ------------------------------------------------------------------------

    async def browse_listings(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.BrowseListingsResponse")
        search, ok = normalize_search(str(request.search))
        if (
            not ok
            or not valid_tab(int(request.tab))
            or not valid_section(int(request.section))
            or not valid_category(int(request.category), int(request.subcategory))
            or not valid_sort(int(request.sort))
        ):
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        if int(request.section) == LISTING_SECTION_AUCTION:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        scope = self._market.scope_enum
        page_size = clamp_page_size(int(request.page_size), self._market.default_page_size, self._market.max_page_size)
        now = self._now_ms()
        zones = self._zones_provider()
        market_zones: tuple[int, ...] = ()
        if scope == MARKET_SCOPE_ZONE:
            zone, code = await self._with_budget(self._resolve_home_zone(original.player_id, "BrowseListings"))
            if code != 0:
                resp.error_message.id = code
                return resp
            market_zones = zones.members(zone)
        elif scope == MARKET_SCOPE_GLOBAL:
            if int(request.zone_filter) != 0:
                market_zones = zones.members(int(request.zone_filter))
        else:
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        title_pat = f"%{escape_like(search)}%" if search else ""
        search_id = int(search) if search.isdigit() else 0
        q = ListingQuery(
            tab=int(request.tab),
            category=int(request.category),
            subcategory=int(request.subcategory),
            sort=int(request.sort),
            now_ms=now,
            market_zones=market_zones,
            title_like_pattern=title_pat,
            search_listing_id=search_id,
            favorites_of=original.player_id if request.favorites_only else 0,
        )
        try:
            async def _run():
                total = await self.store.count_listings(q)
                page, page_count, offset = page_window(total, int(request.page), page_size, self._market.max_page)
                recs = await self.store.query_listings(q, offset, page_size)
                favs = await self.store.favorite_ids(original.player_id, [int(r.listing_id) for r in recs])
                return total, page, page_count, recs, favs

            total, page, page_count, recs, favs = await self._with_budget(_run())
        except Exception as exc:
            logger.error("trade BrowseListings store failed player=%d err=%s", original.player_id, exc)
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        for rec in recs:
            resp.listings.add().CopyFrom(
                to_listing_summary(self.protocol, rec, now, original.player_id, int(rec.listing_id) in favs, zones)
            )
        resp.total_count = clamp_uint32(total)
        resp.page = page
        resp.page_size = page_size
        resp.page_count = page_count
        resp.market_scope = scope
        resp.server_now_ms = now
        return resp

    async def _load_visible_listing(self, caller: int, listing_id: int, now_ms: int, method: str) -> tuple[Message | None, int]:
        try:
            rec = await self.store.get_listing(listing_id)
        except ListingNotFoundError:
            return None, ERR_TRADE_LISTING_NOT_FOUND
        except Exception as exc:
            logger.error("trade %s GetListing failed player=%d err=%s", method, caller, exc)
            return None, ERR_SERVICE_UNAVAILABLE
        if int(rec.seller_player_id) == caller:
            return rec, 0
        scope = self._market.scope_enum
        caller_zone = 0
        if scope == MARKET_SCOPE_ZONE:
            caller_zone, code = await self._resolve_home_zone(caller, method)
            if code != 0:
                return None, code
        if not visible_to_buyer(rec, now_ms, scope, caller_zone, self._zones_provider()):
            return None, ERR_TRADE_LISTING_NOT_FOUND
        return rec, 0

    async def get_listing_detail(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.GetListingDetailResponse")
        lid = int(request.listing_id)
        if lid == 0:
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        now = self._now_ms()
        try:
            rec, code = await self._with_budget(self._load_visible_listing(original.player_id, lid, now, "GetListingDetail"))
            if code != 0:
                resp.error_message.id = code
                return resp
            fav = await self._with_budget(self.store.favorite_exists(original.player_id, lid))
        except Exception as exc:
            logger.error("trade GetListingDetail failed player=%d err=%s", original.player_id, exc)
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        resp.detail.summary.CopyFrom(
            to_listing_summary(self.protocol, rec, now, original.player_id, fav, self._zones_provider())
        )
        resp.detail.description = str(rec.description)
        resp.server_now_ms = now
        return resp

    async def set_favorite(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        lid = int(request.listing_id)
        resp = self.protocol.message("trade.SetFavoriteResponse", listing_id=lid)
        if lid == 0:
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        try:
            if not request.favorite:
                await self._with_budget(self.store.delete_favorite(original.player_id, lid))
                resp.favorite = False
                return resp
            now = self._now_ms()
            _, code = await self._with_budget(self._load_visible_listing(original.player_id, lid, now, "SetFavorite"))
            if code != 0:
                resp.error_message.id = code
                return resp
            if await self._with_budget(self.store.favorite_exists(original.player_id, lid)):
                resp.favorite = True
                return resp
            count = await self._with_budget(self.store.count_favorites(original.player_id))
            if count >= self._market.max_favorites_per_player:
                resp.error_message.id = ERR_TRADE_FAVORITE_LIMIT_REACHED
                return resp
            await self._with_budget(self.store.insert_favorite(original.player_id, lid, now))
            resp.favorite = True
            return resp
        except Exception as exc:
            logger.error("trade SetFavorite failed player=%d err=%s", original.player_id, exc)
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp

    async def get_my_shelf(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.GetMyShelfResponse")
        page_size = clamp_page_size(int(request.page_size), self._market.default_page_size, self._market.max_page_size)
        now = self._now_ms()
        zones = self._zones_provider()
        try:
            async def _run():
                total = await self.store.count_seller_listings(original.player_id)
                page, page_count, offset = page_window(total, int(request.page), page_size, self._market.max_page)
                recs = await self.store.query_seller_listings(original.player_id, offset, page_size)
                favs = await self.store.favorite_ids(original.player_id, [int(r.listing_id) for r in recs])
                return total, page, page_count, recs, favs

            total, page, page_count, recs, favs = await self._with_budget(_run())
        except Exception as exc:
            logger.error("trade GetMyShelf failed player=%d err=%s", original.player_id, exc)
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        for rec in recs:
            resp.listings.add().CopyFrom(
                to_listing_summary(self.protocol, rec, now, original.player_id, int(rec.listing_id) in favs, zones)
            )
        resp.total_count = clamp_uint32(total)
        resp.page = page
        resp.page_size = page_size
        resp.page_count = page_count
        resp.server_now_ms = now
        return resp

    # -- Official Mall & RMB (disabled -> 20003 kTradeFeatureDisabled) ----------------------------------

    async def list_official_skus(self, request: Message, context, path: str) -> Message:
        await self._admit_client(context, path)
        resp = self.protocol.message("trade.ListOfficialSkusResponse")
        resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
        return resp

    async def create_official_order(self, request: Message, context, path: str) -> Message:
        await self._admit_client(context, path)
        return self._order_response(None, tip=ERR_TRADE_FEATURE_DISABLED)

    async def _rmb_disabled(self, context, path: str, resp_name: str, *, with_now: bool = True) -> Message:
        await self._admit_client(context, path)
        resp = self.protocol.message(resp_name)
        resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
        if with_now and "server_now_ms" in resp.DESCRIPTOR.fields_by_name:
            resp.server_now_ms = self._now_ms()
        return resp

    async def create_rmb_consignment(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.RmbConsignmentResponse")

    async def get_rmb_consignment(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.RmbConsignmentResponse")

    async def list_rmb_consignments(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.ListRmbConsignmentsResponse")

    async def buy_rmb_consignment(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.CommerceOrderResponse")

    async def cancel_rmb_consignment(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.RmbConsignmentResponse")

    async def get_rmb_order(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.CommerceOrderResponse")

    async def list_rmb_orders(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.ListCommerceOrdersResponse")

    async def cancel_rmb_order(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.CommerceOrderResponse")

    async def list_seller_incomes(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.ListSellerIncomesResponse")

    async def create_withdrawal(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.WithdrawalResponse", with_now=False)

    async def get_withdrawal(self, request: Message, context, path: str) -> Message:
        return await self._rmb_disabled(context, path, "trade.WithdrawalResponse", with_now=False)

    # -- Core Commerce RPCs (18) --------------------------------------------------------------------

    async def _run_commerce_order(self, context, path: str, fn: Callable[[RoleOriginal, CommerceEngine], Awaitable[Message]]) -> Message:
        original = await self._admit_client(context, path)
        if self.commerce is None:
            return self._order_response(None, tip=ERR_TRADE_FEATURE_DISABLED)
        try:
            rec = await self._with_budget(fn(original, self.commerce))
            return self._order_response(rec)
        except Exception as exc:
            return self._order_response(None, error=exc)

    async def get_commerce_order(self, request: Message, context, path: str) -> Message:
        order_id = str(request.order_id)
        if "-" in order_id and not self._mall_url:
            await self._admit_client(context, path)
            return self._order_response(None, tip=ERR_TRADE_FEATURE_DISABLED)
        async def _call(orig: RoleOriginal, eng: CommerceEngine) -> Message:
            if not order_id.isdigit():
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid order_id")
            return await eng.get(orig, int(order_id))

        return await self._run_commerce_order(context, path, _call)

    async def cancel_commerce_order(self, request: Message, context, path: str) -> Message:
        order_id = str(request.order_id)
        if "-" in order_id and not self._mall_url:
            await self._admit_client(context, path)
            return self._order_response(None, tip=ERR_TRADE_FEATURE_DISABLED)
        async def _call(orig: RoleOriginal, eng: CommerceEngine) -> Message:
            if not order_id.isdigit():
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid order_id")
            return await eng.mutate(orig, int(order_id), "cancel")

        return await self._run_commerce_order(context, path, _call)

    async def list_commerce_orders(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.ListCommerceOrdersResponse", server_now_ms=now)
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        cursor = str(request.cursor)
        if cursor.startswith("mall:"):
            cursor = ""
        limit = min(max(int(request.limit), 1), 100)
        try:
            rows = await self._with_budget(
                self.store.list_commerce_orders(original.player_id, CommerceCursor.parse(cursor), limit)
            )
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
            return resp
        for r in rows:
            resp.next_cursor = CommerceCursor(int(r.created_ms), int(r.commerce_id)).encode()
            try:
                w = decode_workflow(self.protocol, r)
            except Exception:
                continue
            if owns_record(original, r, w):
                resp.orders.add().CopyFrom(self._order_view(r))
        return resp

    async def create_player_trade(self, request: Message, context, path: str) -> Message:
        async def _call(orig: RoleOriginal, eng: CommerceEngine) -> Message:
            buyer_id = int(request.buyer_player_id)
            if self._home_zones is not None and buyer_id != 0:
                zone = await self._home_zones.home_zone(buyer_id)
                if zone == 0:
                    raise TradeRejection(ERR_TRADE_COUNTERPARTY_UNKNOWN, "counterparty unknown")
            return await eng.create_player_trade(orig, request)

        return await self._run_commerce_order(context, path, _call)

    async def confirm_player_trade(self, request: Message, context, path: str) -> Message:
        async def _call(orig: RoleOriginal, eng: CommerceEngine) -> Message:
            oid = str(request.order_id)
            if not oid.isdigit():
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid order_id")
            return await eng.mutate(orig, int(oid), "confirm")

        return await self._run_commerce_order(context, path, _call)

    async def create_consignment(self, request: Message, context, path: str) -> Message:
        return await self._run_commerce_order(context, path, lambda orig, eng: eng.create_consignment(orig, request))

    async def buy_consignment(self, request: Message, context, path: str) -> Message:
        async def _call(orig: RoleOriginal, eng: CommerceEngine) -> Message:
            lid = str(request.listing_id)
            req_id = str(request.client_request_id)
            if not lid.isdigit() or len(req_id) < 8:
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid listing_id or client_request_id")
            return await eng.mutate(orig, int(lid), "buy", req_id)

        return await self._run_commerce_order(context, path, _call)

    async def cancel_consignment(self, request: Message, context, path: str) -> Message:
        async def _call(orig: RoleOriginal, eng: CommerceEngine) -> Message:
            lid = str(request.listing_id)
            if not lid.isdigit():
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid listing_id")
            return await eng.mutate(orig, int(lid), "cancel")

        return await self._run_commerce_order(context, path, _call)

    async def place_market_order(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.MarketOrderResponse")
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        if int(request.market_id) != 1:
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        try:
            rec = await self._with_budget(self.commerce.place_market_order(original, request))
            resp.order.CopyFrom(self._market_view(rec))
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def cancel_market_order(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.MarketOrderResponse")
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        oid = str(request.market_order_id)
        if not oid.isdigit():
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        try:
            rec = await self._with_budget(self.commerce.mutate(original, int(oid), "cancel"))
            resp.order.CopyFrom(self._market_view(rec))
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def list_market_orders(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.ListMarketOrdersResponse")
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        limit = min(max(int(request.limit), 1), 100)
        try:
            rows = await self._with_budget(
                self.store.list_market_orders(original.player_id, CommerceCursor.parse(str(request.cursor)), limit)
            )
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
            return resp
        for r in rows:
            resp.next_cursor = CommerceCursor(int(r.created_ms), int(r.commerce_id)).encode()
            try:
                w = decode_workflow(self.protocol, r)
            except Exception:
                continue
            if owns_record(original, r, w):
                resp.orders.add().CopyFrom(self._market_view(r))
        return resp

    async def get_market_depth(self, request: Message, context, path: str) -> Message:
        await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.GetMarketDepthResponse", server_now_ms=now)
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        if int(request.market_id) != 1 or not self.commerce.policy.currencies:
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        limit = min(max(int(request.limit), 1), 50)
        try:
            asks, bids = await self._with_budget(
                self.store.query_market_depth(1, int(request.item_config_id), self.commerce.policy.currencies[0], limit)
            )
        except Exception as exc:
            logger.error("trade GetMarketDepth failed err=%s", exc)
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        for price, qty in asks:
            lvl = resp.asks.add()
            lvl.unit_price = price
            lvl.quantity = qty
        for price, qty in bids:
            lvl = resp.bids.add()
            lvl.unit_price = price
            lvl.quantity = qty
        return resp

    async def list_trade_markets(self, request: Message, context, path: str) -> Message:
        await self._admit_client(context, path)
        resp = self.protocol.message("trade.ListTradeMarketsResponse")
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        if self.commerce.policy.currencies:
            currency = self.commerce.policy.currencies[0]
            for item_cfg in self.commerce.policy.items:
                entry = resp.markets.add()
                entry.market_id = 1
                entry.item_config_id = item_cfg
                entry.title = self.commerce.policy.item_titles.get(item_cfg, "")
                entry.currency_type = currency
        return resp

    async def create_auction_lot(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.AuctionLotResponse", server_now_ms=now)
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        try:
            rec = await self._with_budget(self.commerce.create_auction_lot(original, request))
            lot_v = self._lot_view(rec)
            if lot_v is not None:
                resp.lot.CopyFrom(lot_v)
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def place_auction_bid(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.AuctionLotResponse", server_now_ms=now)
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        lid = str(request.lot_id)
        if not lid.isdigit():
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        try:
            await self._with_budget(self.commerce.place_auction_bid(original, int(lid), request))
            lot = await self._with_budget(self.commerce.get(original, int(lid)))
            lot_v = self._lot_view(lot)
            if lot_v is not None:
                resp.lot.CopyFrom(lot_v)
            # 出价资金异步冻结，客户端收到 20013 (kTradeAssetDeliveryPending)
            resp.error_message.id = ERR_TRADE_ASSET_DELIVERY_PENDING
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def get_auction_lot(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.AuctionLotResponse", server_now_ms=now)
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        lid = str(request.lot_id)
        if not lid.isdigit():
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        try:
            rec = await self._with_budget(self.commerce.get(original, int(lid)))
            if int(rec.kind) != COMMERCE_KIND_AUCTION_LOT:
                resp.error_message.id = ERR_TRADE_ORDER_NOT_FOUND
                return resp
            lot_v = self._lot_view(rec)
            if lot_v is not None:
                resp.lot.CopyFrom(lot_v)
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def cancel_auction_lot(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.AuctionLotResponse", server_now_ms=now)
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        lid = str(request.lot_id)
        if not lid.isdigit():
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        try:
            rec = await self._with_budget(self.commerce.mutate(original, int(lid), "cancel"))
            lot_v = self._lot_view(rec)
            if lot_v is not None:
                resp.lot.CopyFrom(lot_v)
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def browse_commerce_offers(self, request: Message, context, path: str) -> Message:
        await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.BrowseCommerceOffersResponse", server_now_ms=now)
        if self.commerce is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        kind = int(request.kind)
        if kind not in (COMMERCE_KIND_CONSIGNMENT, COMMERCE_KIND_AUCTION_LOT):
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        after = int(request.cursor) if str(request.cursor).isdigit() else 0
        limit = min(max(int(request.limit), 1), 100)
        try:
            rows = await self._with_budget(self.store.browse_commerce_offers(kind, after, now, limit))
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
            return resp
        titles = self.commerce.policy.item_titles
        for r in rows:
            resp.next_cursor = str(r.commerce_id)
            try:
                w = decode_workflow(self.protocol, r)
            except Exception:
                resp.error_message.id = ERR_SERVICE_UNAVAILABLE
                return resp
            if w.stage not in ("open", "lot_open"):
                continue
            offer = resp.offers.add()
            offer.order.CopyFrom(self._order_view(r))
            if w.HasField("seller_assets"):
                offer.assets.CopyFrom(w.seller_assets)
                for cfg_id, cnt in w.seller_assets.items.items():
                    it = offer.item_instances.add()
                    it.config_id = int(cfg_id)
                    it.count = int(cnt)
                    it.name = titles.get(int(cfg_id), "")
            for l in w.legs:
                if l.role != "seller_debit" or not l.HasField("snapshot"):
                    continue
                for inst in l.snapshot.items:
                    it = offer.item_instances.add()
                    it.instance_id = int(inst.item_uuid)
                    it.config_id = int(inst.config_id)
                    it.count = int(inst.stack_size)
                    it.name = titles.get(int(inst.config_id), "")
                if l.snapshot.HasField("pet") and int(l.snapshot.pet.pet_id) != 0:
                    offer.pet.instance_id = int(l.snapshot.pet.pet_id)
                    offer.pet.config_id = int(l.snapshot.pet.pet_table_id)
                    offer.pet.name = str(l.snapshot.pet.name)
                    offer.pet.level = int(l.snapshot.pet.level)
            if int(r.kind) == COMMERCE_KIND_AUCTION_LOT:
                lot_v = self._lot_view(r)
                if lot_v is not None:
                    offer.auction.CopyFrom(lot_v)
        return resp

    # -- TradeAdmin (3 RPCs) ------------------------------------------------------------------------

    async def seed_listing(self, request: Message, context, path: str) -> Message:
        await self._reject_client_session(context, SERVICE_ADMIN)
        if self._mode not in ("dev", "test"):
            await context.abort(CODE.PERMISSION_DENIED, f"SeedListing is only available in dev/test mode (current {self._mode!r})")
        resp = self.protocol.message("trade.SeedListingResponse")
        if not valid_seed_request(request):
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        seller = int(request.seller_player_id)
        zone, code = await self._with_budget(self._resolve_home_zone(seller, "SeedListing"))
        if code != 0:
            resp.error_message.id = code
            return resp
        if self._listing_ids is None:
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        try:
            lid = await self._with_budget(self._listing_ids())
            if lid == 0:
                resp.error_message.id = ERR_SERVICE_UNAVAILABLE
                return resp
            now = self._now_ms()
            notice_end = now + int(request.notice_duration_ms)
            rec = self.protocol.message(
                "trade.TradeListingRecord",
                listing_id=lid,
                seller_player_id=seller,
                seller_account="",
                market_zone=zone,
                seller_zone_at_listing=zone,
                category=int(request.category),
                subcategory=int(request.subcategory),
                title=str(request.title),
                level=int(request.level),
                price_fen=int(request.price_fen),
                status=LISTING_STATUS_LISTED,
                summary=str(request.summary),
                description=str(request.description),
                icon_key=str(request.icon_key),
                notice_end_ms=notice_end,
                sale_end_ms=notice_end + int(request.sale_duration_ms),
                created_ms=now,
                updated_ms=now,
                version=0,
            )
            await self._with_budget(self.store.insert_listing(rec))
            resp.listing_id = lid
            resp.market_zone = zone
            return resp
        except Exception as exc:
            logger.error("trade SeedListing failed seller=%d err=%s", seller, exc)
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp

    async def list_manual_contracts(self, request: Message, context, path: str) -> Message:
        await self._admit_manual_ops(context, request, path)
        limit = int(request.limit)
        if not 1 <= limit <= MANUAL_LIST_PAGE_MAX:
            await context.abort(CODE.INVALID_ARGUMENT, f"limit must be within [1, {MANUAL_LIST_PAGE_MAX}]")
        resp = self.protocol.message("trade.ListManualContractsResponse", target=self._manual_target)
        try:
            rows = await self._with_budget(
                self.store.list_manual_commerce(int(request.after_updated_ms), int(request.after_commerce_id), limit)
            )
        except Exception as exc:
            logger.error("trade ListManualContracts failed err=%s", exc)
            resp.retry = True
            return resp
        for r in rows:
            resp.contracts.add().CopyFrom(r)
        if len(rows) == limit:
            last = rows[-1]
            resp.next_after_updated_ms = int(last.updated_ms)
            resp.next_after_commerce_id = int(last.commerce_id)
        return resp

    async def resolve_manual_contract(self, request: Message, context, path: str) -> Message:
        await self._admit_manual_ops(context, request, path)
        cid = int(request.commerce_id)
        mode = int(request.mode)
        if cid == 0:
            await context.abort(CODE.INVALID_ARGUMENT, "missing commerce_id")
        if mode not in (_MANUAL_MODE_SHOW, _MANUAL_MODE_DRY_RUN, _MANUAL_MODE_EXECUTE):
            await context.abort(CODE.INVALID_ARGUMENT, "mode must be SHOW, DRY_RUN or EXECUTE")
        out = self.protocol.message("trade.ResolveManualContractResponse", target=self._manual_target)
        assert self.commerce is not None
        if mode == _MANUAL_MODE_SHOW:
            try:
                report = await self._with_budget(self.commerce.show_manual(cid))
            except ManualNotFoundError:
                out.verdict = _VERDICT_NOT_FOUND
                return out
            except Exception as exc:
                logger.error("trade ResolveManualContract SHOW failed cid=%d err=%s", cid, exc)
                out.verdict = _VERDICT_RETRY
                out.refusal = str(exc)
                return out
            out.verdict = _VERDICT_OK
            out.record.CopyFrom(report["record"])
            if report["linked"] is not None:
                out.linked.CopyFrom(report["linked"])
            if report["decode_err"]:
                out.refusal = f"workflow undecodable: {report['decode_err']}"
            for lf in report["legs"]:
                row = out.legs.add()
                row.leg.CopyFrom(lf["leg"])
                row.op_status = lf["op_status"]
                row.final_ms = lf["final_ms"]
                row.read_failed = lf["read_failed"]
            for pf in report["parties"]:
                row = out.parties.add()
                row.party = pf["party"]
                row.player_id = pf["player_id"]
                row.contract_owner_revision = pf["contract_owner_revision"]
                row.current_owner_revision = pf["current_owner_revision"]
                row.same_account = pf["same_account"]
                row.deleted = pf["deleted"]
                row.frozen = pf["frozen"]
                row.unknown = pf["unknown"]
                row.changed = pf["changed"]
            for act, allowed, detail in report["actions"]:
                row = out.actions.add()
                row.action = act
                row.allowed = allowed
                row.detail = detail
            return out

        execute = mode == _MANUAL_MODE_EXECUTE
        if str(request.expect_target) != self._manual_target:
            out.verdict = _VERDICT_REFUSED
            out.refusal = "target_mismatch"
            self._write_manual_audit(request, out, executed=False)
            return out
        try:
            parts = tuple(parse_partial_part(str(raw)) for raw in request.parts)
            mreq = ManualRequest(
                commerce_id=cid,
                expect_version=int(request.expect_version),
                action=str(request.action),
                operator=str(request.operator),
                reason=str(request.reason),
                parts=parts,
                party=str(request.party),
                to_player_id=int(request.to_player_id),
                linked_commerce_id=int(request.linked_commerce_id),
                linked_version=int(request.linked_version),
            )
            res = await self._with_budget(self.commerce.resolve_manual(mreq, execute=execute))
            out.verdict = _VERDICT_OK
            out.stage = res.stage
            out.status = res.status
            if res.audit is not None:
                out.plan.CopyFrom(res.audit)
            if res.linked is not None and res.linked.audit is not None:
                out.linked_plan.CopyFrom(res.linked.audit)
        except ManualNotFoundError:
            out.verdict = _VERDICT_NOT_FOUND
        except ManualVersionChangedError as exc:
            out.verdict = _VERDICT_VERSION_CHANGED
            out.refusal = str(exc)
        except ManualNotAllowedError as exc:
            out.verdict = _VERDICT_REFUSED
            out.refusal = str(exc)
        except Exception as exc:
            logger.error("trade ResolveManualContract failed cid=%d err=%s", cid, exc)
            out.verdict = _VERDICT_RETRY
            out.refusal = str(exc)
        self._write_manual_audit(request, out, executed=(execute and int(out.verdict) == _VERDICT_OK))
        return out

    def _write_manual_audit(self, req: Message, out: Message, *, executed: bool) -> None:
        line = (
            f"[CommerceManual] operator={str(req.operator)!r} commerce_id={int(req.commerce_id)} "
            f"action={str(req.action)!r} mode={int(req.mode)} verdict={int(out.verdict)} "
            f"refusal={str(out.refusal)!r} stage={str(out.stage)} status={int(out.status)} "
            f"target={self._manual_target} replica={self._replica} reason={str(req.reason)!r} executed={executed}"
        )
        out.audit_line = line
        if executed:
            logger.error("%s", line)
        else:
            logger.info("%s", line)

    # -- RoleFenceParticipant (3 RPCs) --------------------------------------------------------------

    async def prepare_role_freeze(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(
                self.store.prepare_role_freeze(request.state, dry_run=bool(request.dry_run))
            )
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "trade PrepareRoleFreeze failed player=%d version=%d dry_run=%s",
                request.state.player_id,
                request.state.version,
                request.dry_run,
                exc_info=True,
            )
            return self.protocol.message("rolefence.PrepareRoleFreezeResponse", retry=True)

    async def apply_role_fence(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(self.store.apply_role_fence(request.state))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "trade ApplyRoleFence failed player=%d version=%d",
                request.state.player_id,
                request.state.version,
                exc_info=True,
            )
            return self.protocol.message("rolefence.ApplyRoleFenceResponse", retry=True)

    async def purge_removed_role(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(self.store.purge_removed_role(request.state))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "trade PurgeRemovedRole failed player=%d version=%d",
                request.state.player_id,
                request.state.version,
                exc_info=True,
            )
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=True)
