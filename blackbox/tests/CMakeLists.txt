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

INCLUDE_DIRECTORIES("." "../include")

INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckCSourceRuns)
INCLUDE(CheckFunctionExists)
INCLUDE(CheckIncludeFile)
INCLUDE(CheckIncludeFiles)
INCLUDE(CheckLibraryExists)
INCLUDE(CheckSymbolExists)
INCLUDE(CheckTypeSize)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
INCLUDE(CheckCXXSourceCompiles)
INCLUDE(CheckCXXSourceRuns)

INCLUDE_DIRECTORIES("." "../include")

# find CHECK package for unit testing (version 0.9.10 or newer)
FIND_PACKAGE(Check)  
IF(CHECK_FOUND)
  CHECK_CXX_SOURCE_RUNS("
#include <check.h>
int main(int argc, char **argv)
{
  if (CHECK_MAJOR_VERSION > 0 
      || (CHECK_MINOR_VERSION > 9
          || (CHECK_MINOR_VERSION == 9 && CHECK_MICRO_VERSION >= 10)))
    return 0;
  return 1; 
}"
    CHECK_VERSION_IS_GOOD)
  IF(CHECK_VERSION_IS_GOOD)
    ADD_EXECUTABLE(unittest unittest.c)
    TARGET_LINK_LIBRARIES(unittest blackbox)
    TARGET_LINK_LIBRARIES(unittest "-lrt")
    TARGET_LINK_LIBRARIES(unittest ${CHECK_LIBRARIES} pthread "-lm")
    ADD_TEST(NAME unittest COMMAND unittest)
    MESSAGE("   The package 'check' was found - unit testing is enabled")
  ELSE(CHECK_VERSION_IS_GOOD)
    MESSAGE("   The version of the package 'check' is too old - unit testing is disabled")
  ENDIF(CHECK_VERSION_IS_GOOD)
ELSE(CHECK_FOUND)
  MESSAGE("   The package 'check' was not found - unit testing is disabled")
ENDIF(CHECK_FOUND)
