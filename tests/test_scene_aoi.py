from xuanming.nodes.scene_aoi import (
    FollowGrant,
    FollowKind,
    FollowSource,
    Policy,
    Priority,
    SceneAoi,
    VisibilityChange,
    effective_capacity,
    hex_at,
    net_changes,
    priority_weight,
)


def test_native_weights_capacity_and_flat_hex():
    assert [priority_weight(p) for p in Priority] == [0, 1, 2, 3, 4, 255, 5]
    assert [priority_weight(p, Policy.DUNGEON) for p in Priority] == [0, 2, 3, 1, 4, 255, 5]
    assert [priority_weight(p, Policy.PVP_ARENA) for p in Priority] == [0, 2, 4, 1, 3, 255, 5]
    assert effective_capacity() == 100
    assert effective_capacity(0) == 20 and effective_capacity(201) == 200
    assert effective_capacity(180, .5) == 110 and effective_capacity(200, 1.) == 20
    assert hex_at((0., 0., 0.)) == (0, 0)
    assert hex_at((30., 17.32, 99.)) == (1, 0)
    assert hex_at((-30., -17.32, -99.)) == (-1, 0)
    assert hex_at((0., 34.64, 0.)) == (0, 1)


def test_spatial_visibility_is_directed_and_uses_3d_radius_at_admission():
    aoi = SceneAoi()
    assert not aoi.enter(10, (0., 0., 0.), radius=5.)
    assert aoi.enter(20, (0., 0., 10.)) == (VisibilityChange(20, 10, True),)
    assert aoi.visible_targets(10) == ()
    assert aoi.visible_targets(20) == (10,)
    assert aoi.visible_watchers(10) == (20,) and aoi.visible_watchers(20) == ()
    assert not aoi.enter(30, (0., 0., 21.))
    assert aoi.leave(10) == (VisibilityChange(20, 10, False),)
    assert not aoi.leave(10) and aoi.visible_targets(20) == ()


def test_native_hex_hysteresis_and_teleport_rebuild():
    aoi = SceneAoi()
    aoi.enter(1, (0., 0., 0.))
    pair = (VisibilityChange(1, 2, True), VisibilityChange(2, 1, True))
    assert aoi.enter(2, (9., 0., 0.)) == pair
    for x in (17., 25.):
        assert not aoi.move(2, (x, 0., 0.))
        assert aoi.sees(1, 2) and aoi.sees(2, 1)
        assert aoi.nearby(1) == set()
    lost = (VisibilityChange(1, 2, False), VisibilityChange(2, 1, False))
    assert aoi.move(2, (65., 0., 0.)) == lost
    assert not aoi.move(2, (12., 0., 0.))
    assert not aoi.move(2, (9., 0., 0.)) and not aoi.sees(1, 2)
    assert not aoi.reset(2, (8., 0., 0.))
    assert aoi.move(2, (8., 0., 0.)) == pair
    assert aoi.reset(2, (9., 0., 0.)) == lost
    assert not aoi.ready(2) and aoi.nearby(1) == set()
    assert aoi.move(2, (9., 0., 0.)) == pair
    assert aoi.ready(2) and aoi.nearby(1) == {2}
    assert not net_changes(pair + lost)


def test_spatial_eviction_is_strict_weight_and_team_upgrade_is_directed():
    aoi = SceneAoi()
    origin = (0., 0., 0.)
    aoi.enter(1, origin, desired_capacity=20, team_id=9)
    for entity in range(2, 22):
        aoi.enter(entity, origin)
    assert aoi.visible_targets(1) == tuple(range(2, 22))
    aoi.enter(22, origin)
    assert not aoi.sees(1, 22) and aoi.sees(22, 1)
    aoi.set_team(21, 9)
    changes = aoi.enter(23, origin, team_id=9)
    assert VisibilityChange(1, 20, False) in changes and VisibilityChange(1, 23, True) in changes
    assert aoi.sees(1, 21) and not aoi.sees(1, 20)
    aoi.set_team(20, 9)
    assert not aoi.sees(1, 20)
    aoi.leave(23)
    assert not aoi.sees(1, 20) and not aoi.sees(1, 22)
    assert len(aoi.visible_targets(1)) == 19


def test_explicit_sources_merge_restore_in_order_and_survive_spatial_reset():
    aoi = SceneAoi()
    aoi.enter(1, (0., 0., 0.), desired_capacity=20)
    source = FollowSource(FollowKind.SKILL, 10)
    for entity in range(2, 24):
        aoi.enter(entity, (100. + 50 * entity, 0., 0.))
    for entity in range(2, 23):
        aoi.follow(1, entity, FollowGrant(source))
    assert aoi.visible_targets(1) == tuple(range(2, 22))
    assert aoi.follow(1, 23, FollowGrant(source, Priority.PINNED)) == (
        VisibilityChange(1, 21, False), VisibilityChange(1, 23, True),
    )
    assert not aoi.sees(1, 21) and not aoi.sees(1, 22)
    assert aoi.unfollow(1, 23, source) == (VisibilityChange(1, 23, False), VisibilityChange(1, 21, True))
    assert aoi.leave(21) == (VisibilityChange(1, 21, False), VisibilityChange(1, 22, True))
    assert not aoi.reset(2, (0., 0., 0.))
    assert VisibilityChange(2, 1, True) in aoi.move(2, (0., 0., 0.))
    assert aoi.reset(2, (200., 0., 0.)) == (VisibilityChange(2, 1, False),)
    assert aoi.sees(1, 2)


def test_stealth_revokes_only_non_detecting_watchers_and_reconciles_sources():
    aoi = SceneAoi()
    origin = (0., 0., 0.)
    for entity in (1, 2, 3):
        aoi.enter(entity, origin)
    team = FollowSource(FollowKind.TEAM, 8)
    scan = FollowSource(FollowKind.SKILL, 8)
    assert not aoi.follow(1, 3, FollowGrant(team, Priority.TEAMMATE))
    assert not aoi.follow(1, 3, FollowGrant(scan, Priority.PINNED, True))
    assert aoi.set_stealth(3, True) == (VisibilityChange(2, 3, False),)
    assert aoi.visible_watchers(3) == (1,)
    assert aoi.unfollow(1, 3, scan) == (VisibilityChange(1, 3, False),)
    assert aoi.visible_watchers(3) == ()
    assert aoi.set_stealth(3, False) == (VisibilityChange(1, 3, True), VisibilityChange(2, 3, True))
    assert not aoi.unfollow(1, 3, team) and aoi.sees(1, 3)
    assert not aoi.follow(3, 3, FollowGrant(scan))
    assert not aoi.follow(3, 999, FollowGrant(scan))
