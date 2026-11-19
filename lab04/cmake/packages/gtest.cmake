CPMAddPackage(
    NAME gtest
    GITHUB_REPOSITORY "google/googletest"
    GIT_TAG v1.18.0
    OPTIONS
    "INSTALL_GTEST OFF"
    "gtest_force_shared_crt ON" # Prevents runtime mismatches on Windows

)
