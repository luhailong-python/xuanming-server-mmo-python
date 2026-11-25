"""trade 三组 gRPC 服务适配层与 fail-closed 鉴权。

对外暴露：
- ``trade.ClientPlayerJubaozhai``: P1 目录 (4) + 核心交易 (18) + 官方商城/人民币 (13) + 经济信用与参考价/额度 (4)
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
from .economy_credit import (
    DAY_MS,
    DEFAULT_COOLING_LOCK_DURATION_MS,
    DEFAULT_MAX_REFERENCE_DEVIATION_PCT,
    DEFAULT_MIN_FRIEND_DURATION_MS,
    MAX_SELLER_REVIEW_COMMENT_RUNES,
    PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED,
    SELLER_RATING_NEGATIVE,
    SELLER_RATING_NEUTRAL,
    SELLER_RATING_POSITIVE,
    DailyPlayerTradeTracker,
    PlayerTradePolicy,
    ReferencePriceReader,
    SellerCreditService,
    SellerReviewRequest,
    StoreDailyPlayerTradeTracker,
    StoreOwnershipLookup,
    StoreReferencePriceReader,
    parse_commerce_order_id,
    seller_credit_view,
    settled_trade_fact,
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
    ERR_TRADE_ORDER_STATE_CONFLICT,
    ERR_TRADE_TEXT_CHECK_UNAVAILABLE,
    ERR_TRADE_TITLE_SENSITIVE,
    InvalidRoleFenceStateError,
    ListingNotFoundError,
    ManualNotAllowedError,
    ManualNotFoundError,
    ManualVersionChangedError,
    TextCheckUnavailableError,
    TradeRejection,
    TradeRoleConflictError,
    commerce_tip_id,
)
from .mall_adapter import MallAdapter
from .model import (
    COMMERCE_KIND_AUCTION_LOT,
    COMMERCE_KIND_BID,
    COMMERCE_KIND_CONSIGNMENT,
    COMMERCE_KIND_OFFICIAL,
    COMMERCE_KIND_RMB_CONSIGNMENT,
    LISTING_SECTION_AUCTION,
    LISTING_STATUS_LISTED,
    MANUAL_LIST_PAGE_MAX,
    MANUAL_RESOLUTION_CALLER,
    MARKET_SCOPE_GLOBAL,
    MARKET_SCOPE_ZONE,
    TEXT_SCENE_TRADE_TITLE,
    TEXT_VERDICT_PASS,
    CommerceCursor,
    ListingQuery,
    MarketLimits,
    MergedZones,
    RoleOriginal,
    clamp_page_size,
    clamp_uint32,
    escape_like,
    is_known_currency,
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
from .rmb import RmbConsignments, local_rmb_view
from .role_trade import ContentGuardChecker, RmbIdentity, RmbIdentityResolver
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

_AUTO = object()


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
        mall: MallAdapter | None = None,
        rmb: RmbConsignments | None = None,
        rmb_identity: RmbIdentityResolver | None = None,
        content_guard: ContentGuardChecker | None = None,
        credits: SellerCreditService | None | object = _AUTO,
        reference_prices: ReferencePriceReader | None | object = _AUTO,
        player_trade_policy: PlayerTradePolicy = PlayerTradePolicy(),
        daily_outflow: DailyPlayerTradeTracker | None | object = _AUTO,
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
        self._mall_url = mall_url or (mall.base_url if mall is not None else "")
        self._rmb_enabled = rmb_enabled
        self._mall = mall
        self._rmb = rmb
        self._rmb_identity = rmb_identity or (RmbIdentity(rmb.directory) if rmb is not None else None)
        self._content_guard = content_guard
        if credits is _AUTO:
            if commerce is not None and commerce.credits is not None:
                self._credits: SellerCreditService | None = commerce.credits
            else:
                self._credits = SellerCreditService(store=store, ownership=StoreOwnershipLookup(store))
        else:
            self._credits = credits  # type: ignore[assignment]
        if reference_prices is _AUTO:
            self._reference_prices: ReferencePriceReader | None = StoreReferencePriceReader(store)
        else:
            self._reference_prices = reference_prices  # type: ignore[assignment]
        self._player_trade_policy = player_trade_policy
        if daily_outflow is _AUTO:
            if (
                commerce is not None
                and commerce.player_trade_guard is not None
                and commerce.player_trade_guard.daily_out is not None
            ):
                self._daily_outflow: DailyPlayerTradeTracker | None = commerce.player_trade_guard.daily_out
            else:
                self._daily_outflow = StoreDailyPlayerTradeTracker(store)
        else:
            self._daily_outflow = daily_outflow  # type: ignore[assignment]
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
                # Economy & Seller Credit (4)
                "GetSellerCredit": self.get_seller_credit,
                "SubmitSellerReview": self.submit_seller_review,
                "GetMarketReferencePrice": self.get_market_reference_price,
                "GetPlayerTradeQuota": self.get_player_trade_quota,
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
            view.designated_buyer_player_id = int(w.designated_buyer_player_id)
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

    # -- Official Mall (2) & RMB Consignment / Withdrawal (11) ------------------------------------------

    async def list_official_skus(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.ListOfficialSkusResponse")
        if self._mall is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        try:
            return await self._with_budget(
                self._mall.list_official_skus(original.player_id, int(request.after_sku_id), int(request.limit))
            )
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
            return resp

    async def create_official_order(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        if self._mall is None:
            return self._order_response(None, tip=ERR_TRADE_FEATURE_DISABLED)
        try:
            return await self._with_budget(self._mall.create_official_order(original, request))
        except Exception as exc:
            return self._order_response(None, error=exc)

    def _rmb_ready(self) -> bool:
        return bool(self._rmb_enabled and self._rmb is not None and self._mall is not None and self._rmb_identity is not None)

    async def _check_trade_title(self, player_id: int, title: str) -> int:
        """对全服可见的人民币寄售标题执行内容安全检查（严禁打印标题原文，故障 fail-closed）。"""
        if self._content_guard is None or not title:
            return 0
        try:
            res = await self._content_guard.check_text(TEXT_SCENE_TRADE_TITLE, player_id, title)
        except TextCheckUnavailableError:
            logger.error("trade content guard unavailable, rmb consignment title refused player=%d", player_id)
            return ERR_TRADE_TEXT_CHECK_UNAVAILABLE
        except Exception:
            logger.error("trade content guard error, rmb consignment title refused player=%d", player_id)
            return ERR_TRADE_TEXT_CHECK_UNAVAILABLE
        if isinstance(res, int):
            verdict, category = res, 0
        else:
            verdict, category = int(res.verdict), int(res.category)
        if verdict == TEXT_VERDICT_PASS:
            return 0
        logger.info("trade rmb consignment title refused by content guard category=%d player=%d", category, player_id)
        return ERR_TRADE_TITLE_SENSITIVE

    async def _rmb_disabled(self, context, path: str, resp_name: str, *, with_now: bool = True) -> Message:
        await self._admit_client(context, path)
        resp = self.protocol.message(resp_name)
        resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
        if with_now and "server_now_ms" in resp.DESCRIPTOR.fields_by_name:
            resp.server_now_ms = self._now_ms()
        return resp

    async def create_rmb_consignment(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.RmbConsignmentResponse")
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.RmbConsignmentResponse", server_now_ms=self._now_ms())
        assert self._rmb is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(
                self._rmb_identity.resolve(original, int(request.character_player_id))
            )
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
            return resp
        title = str(request.title)
        if not title.strip():
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        reject_tip = await self._with_budget(self._check_trade_title(int(owned.role.player_id), title))
        if reject_tip != 0:
            resp.error_message.id = reject_tip
            return resp
        zone, zone_code = await self._with_budget(
            self._resolve_home_zone(int(owned.role.player_id), "CreateRmbConsignment")
        )
        if zone_code != 0 or zone == 0:
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        try:
            rec = await self._with_budget(
                self._rmb.create(
                    original,
                    str(owned.account),
                    int(owned.role.player_id),
                    int(owned.owner_revision),
                    zone,
                    request,
                )
            )
            w = decode_workflow(self.protocol, rec)
            if not w.HasField("rmb"):
                resp.error_message.id = ERR_SERVICE_UNAVAILABLE
                return resp
            if w.rmb.HasField("registered_listing"):
                resp.listing.CopyFrom(w.rmb.registered_listing)
            else:
                resp.listing.CopyFrom(local_rmb_view(self.protocol, rec, w))
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def get_rmb_consignment(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.RmbConsignmentResponse")
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.RmbConsignmentResponse", server_now_ms=self._now_ms())
        assert self._mall is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(self._rmb_identity.resolve(original, 0))
            lid = int(request.listing_id)
            try:
                view = await self._with_budget(self._mall.rmb_get(lid))
            except TradeRejection as rej:
                if rej.tip_id != ERR_TRADE_ORDER_NOT_FOUND:
                    raise
                rec = await self._with_budget(self.store.get_commerce(lid))
                if rec is None or int(rec.kind) != COMMERCE_KIND_RMB_CONSIGNMENT:
                    raise
                w = decode_workflow(self.protocol, rec)
                if not w.HasField("rmb") or str(w.rmb.seller_account) != str(owned.account):
                    raise
                view = local_rmb_view(self.protocol, rec, w)
            resp.listing.CopyFrom(view)
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def list_rmb_consignments(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.ListRmbConsignmentsResponse")
        original = await self._admit_client(context, path)
        assert self._mall is not None and self._rmb_identity is not None
        try:
            await self._with_budget(self._rmb_identity.resolve(original, 0))
            out = await self._with_budget(self._mall.rmb_list(str(request.cursor), int(request.limit)))
            out.server_now_ms = self._now_ms()
            return out
        except Exception as exc:
            resp = self.protocol.message("trade.ListRmbConsignmentsResponse", server_now_ms=self._now_ms())
            resp.error_message.id = commerce_tip_id(exc)
            return resp

    async def buy_rmb_consignment(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.CommerceOrderResponse")
        original = await self._admit_client(context, path)
        assert self._rmb is not None and self._mall is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(self._rmb_identity.resolve(original, 0))
            await self._with_budget(self._rmb.check_buyer_slot(str(owned.account), int(request.listing_id)))
            return await self._with_budget(
                self._mall.rmb_buy(str(owned.account), int(owned.role.player_id), int(owned.owner_revision), request)
            )
        except Exception as exc:
            return self._order_response(None, error=exc)

    async def cancel_rmb_consignment(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.RmbConsignmentResponse")
        original = await self._admit_client(context, path)
        resp = self.protocol.message("trade.RmbConsignmentResponse", server_now_ms=self._now_ms())
        assert self._rmb is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(self._rmb_identity.resolve(original, 0))
            lid = int(request.listing_id)
            await self._with_budget(self._rmb.cancel(original, str(owned.account), lid))
            resp.listing.listing_id = lid
            resp.listing.state = "CANCEL_PENDING"
            resp.listing.phase = "awaiting_escrow_return"
        except Exception as exc:
            resp.error_message.id = commerce_tip_id(exc)
        return resp

    async def _rmb_order_call(self, order_id: str, cancel_order: bool, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.CommerceOrderResponse")
        original = await self._admit_client(context, path)
        assert self._mall is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(self._rmb_identity.resolve(original, 0))
            return await self._with_budget(self._mall.rmb_order(str(owned.account), order_id, cancel=cancel_order))
        except Exception as exc:
            return self._order_response(None, error=exc)

    async def get_rmb_order(self, request: Message, context, path: str) -> Message:
        return await self._rmb_order_call(str(request.order_id), False, context, path)

    async def list_rmb_orders(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.ListCommerceOrdersResponse")
        original = await self._admit_client(context, path)
        assert self._mall is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(self._rmb_identity.resolve(original, 0))
            out = await self._with_budget(
                self._mall.rmb_orders(str(owned.account), str(request.cursor), int(request.limit))
            )
            out.server_now_ms = self._now_ms()
            return out
        except Exception as exc:
            resp = self.protocol.message("trade.ListCommerceOrdersResponse", server_now_ms=self._now_ms())
            resp.error_message.id = commerce_tip_id(exc)
            return resp

    async def cancel_rmb_order(self, request: Message, context, path: str) -> Message:
        return await self._rmb_order_call(str(request.order_id), True, context, path)

    async def list_seller_incomes(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.ListSellerIncomesResponse")
        original = await self._admit_client(context, path)
        assert self._mall is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(self._rmb_identity.resolve(original, 0))
            out = await self._with_budget(
                self._mall.rmb_incomes(str(owned.account), str(request.cursor), int(request.limit))
            )
            out.server_now_ms = self._now_ms()
            return out
        except Exception as exc:
            resp = self.protocol.message("trade.ListSellerIncomesResponse", server_now_ms=self._now_ms())
            resp.error_message.id = commerce_tip_id(exc)
            return resp

    async def create_withdrawal(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.WithdrawalResponse", with_now=False)
        original = await self._admit_client(context, path)
        assert self._mall is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(self._rmb_identity.resolve(original, 0))
            return await self._with_budget(
                self._mall.rmb_withdrawal(
                    str(owned.account),
                    request_id=str(request.client_request_id),
                    amount_fen=int(request.amount_fen),
                )
            )
        except Exception as exc:
            resp = self.protocol.message("trade.WithdrawalResponse")
            resp.error_message.id = commerce_tip_id(exc)
            return resp

    async def get_withdrawal(self, request: Message, context, path: str) -> Message:
        if not self._rmb_ready():
            return await self._rmb_disabled(context, path, "trade.WithdrawalResponse", with_now=False)
        original = await self._admit_client(context, path)
        assert self._mall is not None and self._rmb_identity is not None
        try:
            owned = await self._with_budget(self._rmb_identity.resolve(original, 0))
            return await self._with_budget(
                self._mall.rmb_withdrawal(str(owned.account), withdrawal_id=str(request.withdrawal_id))
            )
        except Exception as exc:
            resp = self.protocol.message("trade.WithdrawalResponse")
            resp.error_message.id = commerce_tip_id(exc)
            return resp

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
        if "-" in order_id:
            original = await self._admit_client(context, path)
            if self._mall is None:
                return self._order_response(None, tip=ERR_TRADE_FEATURE_DISABLED)
            try:
                return await self._with_budget(
                    self._mall.get_or_cancel_official_order(original, order_id, cancel=False)
                )
            except Exception as exc:
                return self._order_response(None, error=exc)

        async def _call(orig: RoleOriginal, eng: CommerceEngine) -> Message:
            if not order_id.isdigit():
                raise TradeRejection(ERR_INVALID_PARAMETER, "invalid order_id")
            return await eng.get(orig, int(order_id))

        return await self._run_commerce_order(context, path, _call)

    async def cancel_commerce_order(self, request: Message, context, path: str) -> Message:
        order_id = str(request.order_id)
        if "-" in order_id:
            original = await self._admit_client(context, path)
            if self._mall is None:
                return self._order_response(None, tip=ERR_TRADE_FEATURE_DISABLED)
            try:
                return await self._with_budget(
                    self._mall.get_or_cancel_official_order(original, order_id, cancel=True)
                )
            except Exception as exc:
                return self._order_response(None, error=exc)

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
        limit = min(max(int(request.limit), 1), 100)
        if self._mall is not None and (cursor == "" or cursor.startswith("mall:")):
            mall_cursor = cursor[len("mall:") :] if cursor.startswith("mall:") else ""
            try:
                mall_orders = await self._with_budget(
                    self._mall.list_official_orders(original, mall_cursor, limit)
                )
            except Exception as exc:
                resp.error_message.id = commerce_tip_id(exc)
                return resp
            if mall_orders:
                for order in mall_orders:
                    resp.orders.add().CopyFrom(order)
                resp.next_cursor = "mall:" + str(mall_orders[-1].order_id)
                return resp
        if cursor.startswith("mall:"):
            cursor = ""
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
        async def _call(orig: RoleOriginal, eng: CommerceEngine) -> Message:
            target_id = int(request.designated_buyer_player_id)
            if target_id != 0 and self._home_zones is not None:
                zone = await self._home_zones.home_zone(target_id)
                if zone == 0:
                    raise TradeRejection(ERR_TRADE_COUNTERPARTY_UNKNOWN, "designated buyer unknown")
            return await eng.create_consignment(orig, request)

        return await self._run_commerce_order(context, path, _call)

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

    # -- Economy & Seller Credit RPCs (4) -----------------------------------------------------------

    async def get_seller_credit(self, request: Message, context, path: str) -> Message:
        """查询指定卖家在当前权威持有周期下的店铺信用档案（严格脱敏，不下发账号/IP/设备）。"""
        original = await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.SellerCreditResponse", server_now_ms=now)
        if self._credits is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        target_id = int(request.seller_player_id) or original.player_id
        try:
            prof = await self._with_budget(self._credits.get_profile(target_id, now))
        except Exception as exc:
            code = commerce_tip_id(exc)
            if code == ERR_SERVICE_UNAVAILABLE:
                logger.error("trade GetSellerCredit failed seller_id=%d err=%s", target_id, exc)
            resp.error_message.id = code
            return resp
        resp.profile.CopyFrom(seller_credit_view(self.protocol, prof))
        return resp

    async def submit_seller_review(self, request: Message, context, path: str) -> Message:
        """买家对官方托管已结案订单在 7 日窗口内提交好评/中评/差评。"""
        original = await self._admit_client(context, path)
        now = self._now_ms()
        resp = self.protocol.message("trade.SubmitSellerReviewResponse", server_now_ms=now)
        if self._credits is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        rating = int(request.rating)
        if rating not in (SELLER_RATING_POSITIVE, SELLER_RATING_NEUTRAL, SELLER_RATING_NEGATIVE):
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        comment = str(request.comment).strip()
        if len(comment) > MAX_SELLER_REVIEW_COMMENT_RUNES:
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        if comment:
            reject_tip = await self._with_budget(self._check_trade_title(original.player_id, comment))
            if reject_tip != 0:
                resp.error_message.id = reject_tip
                return resp
        order_num = parse_commerce_order_id(str(request.order_id))
        if order_num is None:
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        try:
            rec = await self._with_budget(self.store.get_commerce(order_num))
        except Exception as exc:
            logger.error("trade SubmitSellerReview load order=%d failed err=%s", order_num, exc)
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        if rec is None:
            resp.error_message.id = ERR_TRADE_ORDER_NOT_FOUND
            return resp
        try:
            wf = decode_workflow(self.protocol, rec)
        except Exception as exc:
            logger.error("trade SubmitSellerReview decode order=%d failed err=%s", order_num, exc)
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        fact = settled_trade_fact(rec, wf, int(rec.updated_ms))
        if fact is None:
            resp.error_message.id = ERR_TRADE_ORDER_STATE_CONFLICT
            return resp
        if fact.buyer_player_id != original.player_id:
            resp.error_message.id = ERR_TRADE_ORDER_NOT_FOUND
            return resp
        try:
            review_rec, prof, replayed = await self._with_budget(
                self._credits.submit_review(
                    SellerReviewRequest(trade=fact, rating=rating, comment=comment),
                    now,
                )
            )
        except Exception as exc:
            code = commerce_tip_id(exc)
            if code == ERR_SERVICE_UNAVAILABLE:
                logger.error("trade SubmitSellerReview persist order=%d failed err=%s", order_num, exc)
            resp.error_message.id = code
            return resp
        resp.counted_for_score = bool(review_rec.counted_for_score)
        resp.replayed = bool(replayed)
        resp.profile.CopyFrom(seller_credit_view(self.protocol, prof))
        return resp

    async def get_market_reference_price(self, request: Message, context, path: str) -> Message:
        """查询指定标准品在近 7 日真实成交中的中位参考价（p50）及样本量；不足 10 笔明确标记样本不足。"""
        await self._admit_client(context, path)
        now = self._now_ms()
        item_id = int(request.item_config_id)
        currency = int(request.currency_type)
        resp = self.protocol.message(
            "trade.GetMarketReferencePriceResponse",
            item_config_id=item_id,
            currency_type=currency,
            window_days=7,
            server_now_ms=now,
        )
        if item_id == 0 or not is_known_currency(currency):
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        if self._reference_prices is None:
            resp.error_message.id = ERR_TRADE_FEATURE_DISABLED
            return resp
        try:
            ref = await self._with_budget(self._reference_prices.reference_price(item_id, currency, now))
        except Exception as exc:
            logger.error(
                "trade GetMarketReferencePrice item=%d currency=%d failed err=%s",
                item_id,
                currency,
                exc,
            )
            resp.error_message.id = ERR_SERVICE_UNAVAILABLE
            return resp
        resp.median_unit_price = int(ref.median_unit_price)
        resp.sample_count = clamp_uint32(ref.sample_count)
        resp.insufficient_sample = bool(ref.insufficient_sample)
        return resp

    async def get_player_trade_quota(self, request: Message, context, path: str) -> Message:
        """返回当前游戏内当面交易防骗模式、当日累计已用/剩余货币额度与耗材白名单。"""
        original = await self._admit_client(context, path)
        now = self._now_ms()
        currency = int(request.currency_type)
        resp = self.protocol.message(
            "trade.GetPlayerTradeQuotaResponse",
            currency_type=currency,
            server_now_ms=now,
        )
        if not is_known_currency(currency):
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        cfg = self._player_trade_policy
        resp.mode = str(cfg.mode)
        resp.daily_currency_limit = int(cfg.max_daily_currency_amount)
        resp.consumable_item_ids.extend(int(x) for x in cfg.consumable_items)
        min_friend = int(cfg.min_friend_duration_ms)
        if min_friend == 0 and cfg.mode == PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED:
            min_friend = DEFAULT_MIN_FRIEND_DURATION_MS
        resp.min_friend_duration_ms = min_friend
        max_dev = int(cfg.max_reference_deviation_pct)
        if max_dev == 0 and cfg.mode == PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED:
            max_dev = DEFAULT_MAX_REFERENCE_DEVIATION_PCT
        resp.max_reference_deviation_pct = max_dev
        cool_ms = int(cfg.cooling_lock_duration_ms)
        if cool_ms == 0 and cfg.mode == PLAYER_TRADE_MODE_ACQUAINTANCE_LOCKED:
            cool_ms = DEFAULT_COOLING_LOCK_DURATION_MS
        resp.cooling_lock_duration_ms = cool_ms

        used = 0
        if self._daily_outflow is not None:
            day_start = (now // DAY_MS) * DAY_MS
            try:
                used = await self._with_budget(
                    self._daily_outflow.daily_currency_outflow(original.player_id, currency, day_start)
                )
            except Exception as exc:
                logger.error(
                    "trade GetPlayerTradeQuota player=%d currency=%d failed err=%s",
                    original.player_id,
                    currency,
                    exc,
                )
                resp.error_message.id = ERR_SERVICE_UNAVAILABLE
                return resp
        resp.daily_currency_used = int(used)
        if int(resp.daily_currency_limit) > int(used):
            resp.daily_currency_remaining = int(resp.daily_currency_limit) - int(used)
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
