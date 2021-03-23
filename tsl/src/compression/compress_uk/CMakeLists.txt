set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/compress_uk_build.c
  ${CMAKE_CURRENT_SOURCE_DIR}/compress_uk.c
  ${CMAKE_CURRENT_SOURCE_DIR}/compress_uk_fns.c
)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
#target_include_directories(${TSL_LIBRARY_NAME} PRIVATE ${CMAKE_SOURCE_DIR})

