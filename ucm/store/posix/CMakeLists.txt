file(GLOB_RECURSE UCM_POSIX_STORE_CC_SOURCE_FILES "./cc/*.cc")
add_library(posixstore SHARED ${UCM_POSIX_STORE_CC_SOURCE_FILES})
target_include_directories(posixstore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cc)
target_link_libraries(posixstore PUBLIC storeintf infra_logger metrics)
if(BUILD_UNIT_TESTS)
    target_compile_definitions(posixstore PUBLIC UCM_ENABLE_TEST_HOOKS)
endif()

# NDS moves KV cache straight from device HBM to the storage backend. It only
# exists on Ascend hosts with the NDS filesystem library installed, so the
# engine is compiled in when both the header and the library are found, and the
# nds_*.cc sources compile to nothing otherwise.
#
# The DataTurbo NDS package installs outside the CANN tree -- headers under
# lib/include and libndsfs.so under lib, per the layout the standalone nds_test
# build links against. Override with -DNDS_ROOT=... or the NDS_ROOT env var when
# it lives elsewhere; the CANN tree is searched too, in case a build ships the
# header alongside the toolkit.
if(DEFINED ENV{NDS_ROOT})
    set(NDS_ROOT_DEFAULT "$ENV{NDS_ROOT}")
else()
    set(NDS_ROOT_DEFAULT "/opt/oceanstor/dataturbo-nds")
endif()
set(NDS_ROOT ${NDS_ROOT_DEFAULT} CACHE PATH "Path to NDS install root")
find_path(NDS_INCLUDE_DIR
    NAMES nds_api.h
    HINTS ${NDS_ROOT} ${ASCEND_ROOT} /usr/local/Ascend/ascend-toolkit/latest
    PATH_SUFFIXES lib/include include aarch64-linux/include x86_64-linux/include
)
find_library(NDS_LIBRARY
    NAMES ndsfs
    HINTS ${NDS_ROOT} ${ASCEND_ROOT} /usr/local/Ascend/ascend-toolkit/latest
    PATH_SUFFIXES lib lib64 aarch64-linux/lib64 x86_64-linux/lib64
)
if(NDS_INCLUDE_DIR AND NDS_LIBRARY AND UCM_RUNTIME_ASCEND_FAMILY)
    message(STATUS "posixstore: nds io engine enabled")
    message(STATUS "  * header: ${NDS_INCLUDE_DIR}/nds_api.h")
    message(STATUS "  * library: ${NDS_LIBRARY}")
    target_compile_definitions(posixstore PUBLIC UCM_ENABLE_NDS=1)
    target_include_directories(posixstore PRIVATE ${NDS_INCLUDE_DIR})
    target_link_libraries(posixstore PRIVATE ${NDS_LIBRARY})
    # NdsQueue binds the device context on each worker thread via Trans::Device.
    target_link_libraries(posixstore PRIVATE trans)
    # libndsfs.so lives outside the default loader path, so record where it was
    # found. Without this the import fails at dlopen time unless the caller has
    # already exported LD_LIBRARY_PATH; the rpath keeps that export optional.
    get_filename_component(NDS_LIBRARY_DIR ${NDS_LIBRARY} DIRECTORY)
    set_property(TARGET posixstore APPEND PROPERTY BUILD_RPATH ${NDS_LIBRARY_DIR})
    set_property(TARGET posixstore APPEND PROPERTY INSTALL_RPATH ${NDS_LIBRARY_DIR})
else()
    message(STATUS "posixstore: nds io engine disabled")
    if(NOT UCM_RUNTIME_ASCEND_FAMILY)
        message(STATUS "  * Requires RUNTIME_ENVIRONMENT=ascend or ascend-a3")
    endif()
    if(NOT NDS_INCLUDE_DIR)
        message(STATUS "  * Missing: nds_api.h (searched NDS_ROOT=${NDS_ROOT})")
    endif()
    if(NOT NDS_LIBRARY)
        message(STATUS "  * Missing: libndsfs (searched NDS_ROOT=${NDS_ROOT})")
    endif()
endif()

file(RELATIVE_PATH INSTALL_REL_PATH ${UCM_ROOT_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
install(TARGETS posixstore LIBRARY DESTINATION ${INSTALL_REL_PATH} COMPONENT ucm)
