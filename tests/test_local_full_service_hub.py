"""单进程本地模式全微服务聚合枢纽（LocalServiceHub）端到端测试。"""

from __future__ import annotations

import asyncio
import json

from test_monopoly import TOKEN, _enter, _settings
from xuanming.runtime import Runtime
from xuanming.smoke import connect


async def _call(client, call_id: int, message_name: str, **fields):
    p = client.protocol
    message_id = p.message_ids[message_name]
    request = p.message(p.routes[message_id].request_type, **fields)
    return await client.rpc(message_id, call_id, request)


def test_local_single_process_routes_all_client_microservices(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            assert runtime.local_services is not None
            assert runtime.scene.battle._match_call is not None
            assert runtime.scene.rename._content_guard_call is not None

            async with _enter(runtime, account="alpha") as (client_a, player_a):
                async with _enter(runtime, account="beta") as (client_b, player_b):
                    assert player_a != player_b

                    # 1. Login: RefreshToken
                    refreshed = await _call(client_a, 10, "ClientPlayerLoginRefreshToken", refresh_token=TOKEN)
                    assert not refreshed.error_message.id
                    assert refreshed.access_token == TOKEN
                    assert refreshed.access_token_expire > 0

                    # 2. Friend: AddFriend -> AcceptFriend -> GetFriendList
                    added = await _call(
                        client_a,
                        11,
                        "ClientPlayerFriendAddFriend",
                        target_player_id=player_b,
                    )
                    assert not added.error_message.id
                    pending = await _call(client_b, 12, "ClientPlayerFriendGetPendingRequests")
                    assert not pending.error_message.id
                    assert any(req.from_player_id == player_a for req in pending.requests)
                    req_id = next(req.request_id for req in pending.requests if req.from_player_id == player_a)
                    accepted = await _call(
                        client_b,
                        13,
                        "ClientPlayerFriendAcceptFriend",
                        from_player_id=player_a,
                        request_id=req_id,
                    )
                    assert not accepted.error_message.id
                    friends_a = await _call(client_a, 14, "ClientPlayerFriendGetFriendList")
                    assert not friends_a.error_message.id
                    assert any(entry.friend_player_id == player_b for entry in friends_a.friends)

                    # 3. Chat: SendChat (world) -> PullChatHistory
                    sent = await _call(
                        client_a,
                        15,
                        "ClientPlayerChatSendChat",
                        request_id="chat-local-1",
                        message={"channel": 1, "content": "单进程世界频道测试"},
                    )
                    assert not sent.error_message.id
                    history = await _call(
                        client_b,
                        16,
                        "ClientPlayerChatPullChatHistory",
                        channel=1,
                        limit=20,
                    )
                    assert not history.error_message.id
                    assert any("单进程世界频道测试" in msg.content for msg in history.messages)

                    # 4. Mail: SceneMailOutbox 直投本地 MailService -> ListMails -> ReadMail
                    valid_item_id = next(iter(runtime.local_services.tables["Item"].rows())).id
                    entry = runtime.protocol.message(
                        "MailOutboxEntry",
                        request_id=700001,
                        source=1,
                        title="本地战斗溢出奖励",
                        body="测试单进程本地邮件直投",
                    )
                    entry.items.add(config_id=valid_item_id, count=2)
                    mail_reply = await runtime.local_services.send_personal_mail(player_a, entry)
                    assert not mail_reply.error_message.id
                    listed = await _call(client_a, 17, "ClientPlayerMailListMails", limit=20)
                    assert not listed.error_message.id
                    assert any(m.title == "本地战斗溢出奖励" for m in listed.mails)
                    mail_id = next(m.mail_id for m in listed.mails if m.title == "本地战斗溢出奖励")
                    read = await _call(client_a, 18, "ClientPlayerMailReadMail", mail_id=mail_id)
                    assert not read.error_message.id
                    assert read.mail.title == "本地战斗溢出奖励"

                    # 5. Guild: CreateGuild -> GetPlayerGuild -> GetGuild
                    created_guild = await _call(
                        client_a,
                        19,
                        "GuildServiceCreateGuild",
                        name="玄冥剑宗",
                    )
                    assert not created_guild.error_message.id
                    guild_id = created_guild.guild.guild_id
                    assert guild_id > 0
                    my_guild = await _call(client_a, 20, "GuildServiceGetPlayerGuild")
                    assert not my_guild.error_message.id
                    assert my_guild.guild.guild_id == guild_id
                    fetched_guild = await _call(client_b, 21, "GuildServiceGetGuild", guild_id=guild_id)
                    assert not fetched_guild.error_message.id
                    assert fetched_guild.guild.name == "玄冥剑宗"

                    # 6. Team: CreateTeam -> GetMyTeam
                    created_team = await _call(client_a, 22, "ClientPlayerTeamCreateTeam", request_id="team-local-1")
                    assert not created_team.error_message.id
                    assert created_team.team.team_id > 0
                    my_team = await _call(client_a, 23, "ClientPlayerTeamGetMyTeam")
                    assert not my_team.error_message.id
                    assert my_team.team.team_id == created_team.team.team_id

                    # 7. Match (Queue + Ranked + Tournament)
                    queue_status = await _call(client_a, 24, "MatchServiceGetQueueStatus")
                    assert queue_status.state == 5  # QUEUE_STATE_NOT_QUEUED
                    ranked_profile = await _call(client_a, 25, "MatchServiceGetRankedProfile")
                    assert not ranked_profile.error_message.id
                    tournament_overview = await _call(client_a, 26, "MatchServiceGetTournamentOverview")
                    assert not tournament_overview.error_message.id
                    squad = await _call(
                        client_a,
                        27,
                        "MatchServiceCreateTournamentSquad",
                        squad_name="剑宗战队",
                    )
                    assert not squad.error_message.id
                    assert squad.squad.name == "剑宗战队"

                    # 8. Rank: GetRankPage
                    rank_page = await _call(
                        client_a,
                        28,
                        "ClientPlayerRankGetRankPage",
                        board={"board_type": 1, "scope": 1, "scope_id": 1},
                        page=1,
                        page_size=10,
                    )
                    assert rank_page.error_message.id != 1003

                    # 9. Payment: ListPayChannels
                    pay_channels = await _call(client_a, 29, "ClientPlayerPaymentListPayChannels", platform=1)
                    assert not pay_channels.error_message.id

                    # 10. Trade (聚宝斋): BrowseListings + GetMyShelf
                    listings = await _call(
                        client_a,
                        30,
                        "ClientPlayerJubaozhaiBrowseListings",
                        tab=1,
                        section=1,
                        category=1,
                        sort=1,
                        page=1,
                        page_size=10,
                    )
                    assert not listings.error_message.id
                    shelf = await _call(client_a, 31, "ClientPlayerJubaozhaiGetMyShelf")
                    assert not shelf.error_message.id

    asyncio.run(exercise())


def test_local_login_persists_created_roles_across_restart(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)
        accounts_file = tmp_path / "local_accounts.json"

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            p, gate = runtime.protocol, runtime.gate
            async with connect(p, gate.info.endpoint) as client:
                await client.send(gate.issue_ticket())
                assert (await client.receive("ClientTokenVerifyResponse")).success
                login = await _call(client, 1, "ClientPlayerLoginLogin", account="persist-user", password=TOKEN)
                assert not login.error_message.id and len(login.players) == 1
                created = await _call(
                    client,
                    2,
                    "ClientPlayerLoginCreatePlayer",
                    name="第二剑修",
                    class_id=2,
                    gender=2,
                )
                assert not created.error_message.id and len(created.players) == 2
                second_id = created.players[1].player.player_id

        assert accounts_file.exists()
        doc = json.loads(accounts_file.read_text(encoding="utf-8"))
        assert len(doc["accounts"]["persist-user"]) == 2

        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            p, gate = runtime.protocol, runtime.gate
            async with connect(p, gate.info.endpoint) as client:
                await client.send(gate.issue_ticket())
                assert (await client.receive("ClientTokenVerifyResponse")).success
                login = await _call(client, 1, "ClientPlayerLoginLogin", account="persist-user", password=TOKEN)
                assert not login.error_message.id
                assert len(login.players) == 2
                assert login.players[1].player.player_id == second_id
                assert login.players[1].player.name == "第二剑修"

    asyncio.run(exercise())
