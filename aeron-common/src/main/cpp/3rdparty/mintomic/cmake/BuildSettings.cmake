if(${MSVC})
    # Enable debug info in Release.
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /Zi")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /Zi")
    set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS_RELEASE} /debug")
elseif(${UNIX})
    # These are all required on Xcode 4.5.1 + iOS, because the defaults are no good.
    set(CMAKE_C_FLAGS "-pthread -g")
    set(CMAKE_CXX_FLAGS "-pthread -g")
    set(CMAKE_C_FLAGS_DEBUG "-O0")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0")
    set(CMAKE_C_FLAGS_RELEASE "-Os")
    set(CMAKE_CXX_FLAGS_RELEASE "-Os")
endif()

if(${IOS})
    set(CMAKE_XCODE_EFFECTIVE_PLATFORMS "-iphoneos;-iphonesimulator")
    set_target_properties(${PROJECT_NAME} PROPERTIES XCODE_ATTRIBUTE_CODE_SIGN_IDENTITY "iPhone Developer")
endif()
