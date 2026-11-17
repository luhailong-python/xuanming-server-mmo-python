TEST(BattleScriptOracle, DumpCombo) {
    auto provider = MakeProvider();
    const auto request = ComboRequest();
    TurnBattleEngine engine(provider);
    ASSERT_TRUE(engine.Initialize(request));
    SetAllAuto(engine, request);
    const auto hex = [](const std::string& bytes) {
        constexpr char digits[] = "0123456789abcdef";
        std::string out;
        for (unsigned char c : bytes) { out += digits[c >> 4]; out += digits[c & 15]; }
        return out;
    };
    std::cout << "ORACLE_REQUEST=" << hex(request.SerializeAsString()) << '\n';
    std::cout << "ORACLE_STATE=" << hex(engine.BuildStateSnapshot().SerializeAsString()) << '\n';
    for (int i = 0; i < 2; ++i) {
        std::cout << "ORACLE_ROUND" << i + 1 << "=" << hex(engine.ResolveCurrentRound().SerializeAsString()) << '\n';
    }
}
