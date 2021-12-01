# TOP-LEVEL
cmake_minimum_required(VERSION 3.12)
project(NGV)

option(ENABLE_PYTHON2 "Enable python2 compile" OFF)
option(ENABLE_CUSTOM_FIND_PYTHON "Find python2/3 without using cmake findpython" OFF)

message(STATUS "Options:\n\nENABLE_PYTHON2: ${ENABLE_PYTHON2}\n")
message(STATUS "ENABLE_CUSTOM_FIND_PYTHON: ${ENABLE_CUSTOM_FIND_PYTHON}\n")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/" "${CMAKE_SOURCE_DIR}/local/lib/cmake/")
set(CMAKE_FIND_FRAMEWORK "LAST")

set(FEATURE_FLAGS "")
set(PLATFORM_LIBS "")

find_library(LIBPTHREAD		pthread)

set(CCSHM_STD_LIBS
		${LIBPTHREAD}
)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
	set(PLATFORM_DEFINE_FLAG "-DDARWIN")
	set(PLATFORM_CXXFLAGS "-std=c++11")
endif()
if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
	set(PLATFORM_DEFINE_FLAG "-DLINUX -D__STDC_CONSTANT_MACROS -D__STDC_LIMIT_MACROS -Wno-multichar")
	set(PLATFORM_LINKER_FLAG "-export-dynamic")
	set(PLATFORM_CXXFLAGS "-fpermissive -std=c++11")
endif()

set(CMAKE_CXX_FLAGS "-g -O3 -Wall -Wno-strict-aliasing ${PLATFORM_CXXFLAGS}")

# The basic idea here is to add all directories that contain source to the include path.
set(CCSHM_INCLUDE
	${CMAKE_SOURCE_DIR}
)

include_directories(${CCSHM_INCLUDE})

set(CCSHM_SHARED_FILES
	EventBus.cxx
	SemRWLock.cxx
	SharedMem.cxx
)

set(CCSHM_FILES
	CCshm.cxx
)

set(CCSHM_PY_FILES
	CCshm_py.cxx
)

set(CMAKE_INSTALL_PREFIX /usr/local/)

# CCshm
add_executable(CCshm
	${CCSHM_FILES}
	${CCSHM_SHARED_FILES}
)
set_target_properties(CCshm
	PROPERTIES
		COMPILE_FLAGS "${PLATFORM_DEFINE_FLAG} ${FEATURE_FLAGS}"
		LINK_FLAGS "${PLATFORM_LINKER_FLAG}"
)
target_link_libraries(CCshm ${CCSHM_STD_LIBS} ${PLATFORM_LIBS})

if (ENABLE_CUSTOM_FIND_PYTHON)
    if (ENABLE_PYTHON2)
        execute_process(COMMAND python2-config --ldflags OUTPUT_VARIABLE TMP)
        message(STATUS "python2 libs: ${TMP}")
        string(STRIP ${TMP} Python2_LIBRARIES)
        execute_process(COMMAND python2-config --cflags OUTPUT_VARIABLE TMP)
        message(STATUS "python2 cflags: ${TMP}")
        string(STRIP ${TMP} Python2_CUSTOM_CFLAGS)
    else()
        message(STATUS "Not compiling python2 module")
    endif()

    execute_process(COMMAND python3-config --ldflags --embed OUTPUT_VARIABLE TMP RESULT_VARIABLE EXIT_CODE ERROR_QUIET)
    message(STATUS "Command returned ${EXIT_CODE}")
    if (EXIT_CODE AND NOT EXIT_CODE EQUAL 0)
        message(STATUS "Executing without --embed for ldflags")
        execute_process(COMMAND python3-config --ldflags OUTPUT_VARIABLE TMP)
    endif()
    message(STATUS "python3 libs: ${TMP}")
    string(STRIP ${TMP} Python3_LIBRARIES)
    execute_process(COMMAND python3-config --cflags OUTPUT_VARIABLE TMP)
    message(STATUS "python3 cflags: ${TMP}")
    string(STRIP ${TMP} Python3_CUSTOM_CFLAGS)
    
    set(Python3_INCLUDE_DIRS "")
else()
    set(Python3_CUSTOM_CFLAGS "")
    find_package(Python3 COMPONENTS Development)
    if (ENABLE_PYTHON2)
        find_package(Python2 COMPONENTS Development)
    endif()
endif()

set(CMAKE_SHARED_LIBRARY_PREFIX "")

# CCshm_py3.so
add_library(CCshm_py3 MODULE
	${CCSHM_PY_FILES}
	${CCSHM_SHARED_FILES}
)
set_target_properties(CCshm_py3
	PROPERTIES
		COMPILE_FLAGS "${PLATFORM_DEFINE_FLAG} ${FEATURE_FLAGS} -DCCSHM_VERSION=3 ${Python3_CUSTOM_CFLAGS}"
		LINK_FLAGS "${PLATFORM_LINKER_FLAG} ${Python3_LINK_OPTIONS}"
		PREFIX ""
		SUFFIX ".so"
)
target_link_libraries(CCshm_py3 ${CCSHM_STD_LIBS} ${PLATFORM_LIBS} ${Python3_LIBRARIES})
target_include_directories(CCshm_py3 PUBLIC ${Python3_INCLUDE_DIRS})

# CCshm_py2.so
if (ENABLE_PYTHON2)
    add_library(CCshm_py2 MODULE
        ${CCSHM_PY_FILES}
        ${CCSHM_SHARED_FILES}
    )
    set_target_properties(CCshm_py2
        PROPERTIES
            COMPILE_FLAGS "${PLATFORM_DEFINE_FLAG} ${FEATURE_FLAGS} -DCCSHM_VERSION=2 ${Python2_CUSTOM_CFLAGS}"
            LINK_FLAGS "${PLATFORM_LINKER_FLAG}"
            PREFIX ""
            SUFFIX ".so"
    )
    target_link_libraries(CCshm_py2 ${CCSHM_STD_LIBS} ${PLATFORM_LIBS} ${Python2_LIBRARIES})
    target_include_directories(CCshm_py2 PUBLIC ${Python2_INCLUDE_DIRS})
    
    # ByteArrayDebug.so
    add_library(ByteArrayDebug MODULE
        ByteArrayDebug.cxx
    )
    set_target_properties(ByteArrayDebug
        PROPERTIES
            COMPILE_FLAGS "${PLATFORM_DEFINE_FLAG} ${FEATURE_FLAGS}"
            LINK_FLAGS "${PLATFORM_LINKER_FLAG}"
            PREFIX ""
            SUFFIX ".so"
    )
    target_link_libraries(ByteArrayDebug ${CCSHM_STD_LIBS} ${PLATFORM_LIBS} ${Python2_LIBRARIES})
    target_include_directories(ByteArrayDebug PUBLIC ${Python2_INCLUDE_DIRS})
endif()
