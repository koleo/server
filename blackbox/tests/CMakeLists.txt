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

CHECK_CXX_SOURCE_COMPILES("
#include <check.h>
int main(int argc, char **argv)
{
  int rcode=5;
  ch_assert_int_eq(rcode, 0);
}"
CHECK_FUNCTIONS)

IF (CHECK_FUNCTIONS)
  # find CHECK package for unit testing
  FIND_PACKAGE(Check REQUIRED)
  ADD_EXECUTABLE(unittest unittest.c)
  TARGET_LINK_LIBRARIES(unittest blackbox)
  TARGET_LINK_LIBRARIES(unittest "-lrt")
  TARGET_LINK_LIBRARIES(unittest ${CHECK_LIBRARIES} pthread)
  ADD_TEST(NAME unittest COMMAND unittest)
ENDIF(CHECK_FUNCTIONS)
