# Copyright (c) 2018, Codership Oy. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# Options
OPTION(BLACKBOX_WITH_ASAN "Build with ASAN instrumentation" OFF)
OPTION(BLACKBOX_WITH_COVERAGE "Build with test coverage" OFF)
OPTION(BLACKBOX_MAINTAINER_MODE "Fail compilation on warnings" OFF)

# C compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")
IF (BLACKBOX_MAINTAINER_MODE)
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
ENDIF()
IF (BLACKBOX_WITH_ASAN)
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
ENDIF()
# To produce coverage report, call cmake with -DWITH_COVERAGE:BOOL=ON
# and run
#   make
#   make test
#   make coverage_report
# The coverage report will be writeen in directory coverage_report.
IF (BLACKBOX_WITH_COVERAGE)
  SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
  SET(CMAKE_EXE_LINKER_FLAGS
    "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
  ADD_CUSTOM_TARGET(coverage_report
    lcov --capture --directory src --directory include --output lcov.info --no-external
    COMMAND genhtml --output-directory coverage_report lcov.info
  )
ENDIF()

# Custom cmake modules
SET(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Black Box library and bbtool
ADD_SUBDIRECTORY(src)
# unittests
IF (BLACKBOX_WITH_COVERAGE)
  ENABLE_TESTING()
  ADD_SUBDIRECTORY(tests)
ENDIF()
