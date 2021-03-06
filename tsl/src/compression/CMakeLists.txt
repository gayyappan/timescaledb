set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/array.c
  ${CMAKE_CURRENT_SOURCE_DIR}/compression.c
  ${CMAKE_CURRENT_SOURCE_DIR}/create.c
  ${CMAKE_CURRENT_SOURCE_DIR}/compress_utils.c
  ${CMAKE_CURRENT_SOURCE_DIR}/datum_serialize.c
  ${CMAKE_CURRENT_SOURCE_DIR}/deltadelta.c
  ${CMAKE_CURRENT_SOURCE_DIR}/dictionary.c
  ${CMAKE_CURRENT_SOURCE_DIR}/gorilla.c
  ${CMAKE_CURRENT_SOURCE_DIR}/segment_meta.c
  ${CMAKE_CURRENT_SOURCE_DIR}/compress_index_map.c
  ${CMAKE_CURRENT_SOURCE_DIR}/compress_uk_build.c
)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
