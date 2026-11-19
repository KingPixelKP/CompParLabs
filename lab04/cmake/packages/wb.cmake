CPMAddPackage(
    NAME wb
    GITHUB_REPOSITORY "abduld/libwb"
    GIT_TAG master
)
target_include_directories(wb PUBLIC ${wb_SOURCE_DIR})