cmake_minimum_required(VERSION 3.22)
project(battle_head_oracle CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
if(NOT DEFINED NATIVE_ROOT)
  message(FATAL_ERROR "NATIVE_ROOT must point to the C++ checkout")
endif()
set(ROOT "${NATIVE_ROOT}")
set(THIRD "${ROOT}/third_party")
set(GEN "${CMAKE_CURRENT_SOURCE_DIR}/cpp/generated")
set(BATTLE "${CMAKE_CURRENT_SOURCE_DIR}/cpp/libs/services/battle")
add_executable(battle_head_oracle
  cpp/tests/turn_battle_engine_test/battle_script_test.cpp
  cpp/libs/services/battle/system/battle_script.cpp
  cpp/libs/services/battle/system/turn_battle_engine.cpp
  cpp/generated/proto/battle/battle_data.pb.cc
  cpp/generated/proto/battle/battle_node.pb.cc
  cpp/generated/proto/battle/player_battle.pb.cc
  cpp/generated/proto/common/component/actor_comp.pb.cc
  cpp/generated/proto/common/component/player_battle_result_comp.pb.cc
  cpp/generated/proto/common/component/player_pet_comp.pb.cc
  cpp/generated/proto/scene/player_pet.pb.cc
  cpp/generated/table/proto/dungeon_table.pb.cc
  cpp/generated/table/proto/monster_table.pb.cc
  cpp/generated/table/proto/pet_table.pb.cc)
target_include_directories(battle_head_oracle PRIVATE
  ${BATTLE} ${GEN} cpp/libs cpp/libs/engine cpp/libs/engine/core cpp/libs/engine/config
  cpp/libs/engine/modules cpp/libs/engine/infra cpp/tests/turn_battle_engine_test
  ${THIRD} ${THIRD}/boost ${THIRD}/zlib ${THIRD}/redis ${THIRD}/muduo
  ${THIRD}/muduo/contrib/windows/include ${THIRD}/redis/deps ${THIRD}/abseil-cpp
  ${THIRD}/ue5navmesh/Public ${THIRD}/spdlog/include ${THIRD}/hexagons_grids
  ${THIRD}/protobuf/src ${THIRD}/protobuf/install_vs2026_dbg/include ${THIRD}/xxhash ${THIRD}/librdkafka/src-cpp
  ${THIRD}/googletest/googletest/include ${THIRD}/googletest/googlemock/include
  ${THIRD}/boringssl/install_vs2026_dbg/include)
target_compile_definitions(battle_head_oracle PRIVATE WIN32 _CRT_SECURE_NO_WARNINGS _WINSOCK_DEPRECATED_NO_WARNINGS _CRT_SECURE_NO_DEPRECATE _CRT_NONSTDC_NO_DEPRECATE ENTT_ID_TYPE=uint64_t _TCHAR_DEFINED UNICODE _SILENCE_ALL_CXX23_DEPRECATION_WARNINGS)
target_compile_options(battle_head_oracle PRIVATE /utf-8 /bigobj)
file(GLOB libs "${ROOT}/lib/*.lib" "${THIRD}/protobuf/install_vs2026_dbg/lib/*.lib" "${THIRD}/boringssl/install_vs2026_dbg/lib/*.lib" "${THIRD}/googletest/install_vs2026_dbg/lib/*.lib" "${THIRD}/nghttp2/install_vs2026_dbg/lib/*.lib")
list(FILTER libs EXCLUDE REGEX "/(battle|proto|table)\\.lib$")
target_link_libraries(battle_head_oracle PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/proto-head-filtered.lib ${CMAKE_CURRENT_SOURCE_DIR}/table-head-filtered.lib ${libs} ws2_32 crypt32 iphlpapi shlwapi)
