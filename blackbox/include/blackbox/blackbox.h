/* Copyright (C) 2019 Codership Oy <info@codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License along
   with this program; if not, write to the Free Software Foundation, Inc.,
   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */


#ifndef BLACKBOX_H
#define BLACKBOX_H

#include <stdlib.h>


#ifdef __cplusplus
extern "C" {
#endif


/* Black Box state codes */
#define BB_STATE_CLOSED       0
#define BB_STATE_DISABLED     1
#define BB_STATE_OPERATIONAL  2


/* error codes specific to the Black Box */
#define BB_ERROR_SIZE          2001
#define BB_ERROR_FORMAT        2002
#define BB_ERROR_OPEN_ALREADY  2003
#define BB_ERROR_CLOSED        2004
#define BB_ERROR_INTERNAL      2005


/*
   Creates a new BB object. If a BB object with the given name already
   exists, it is dumped to a BB dump file in the dumpdir and deleted
   before a new BB object is created. This function must be called
   before calling bb_write().

   This function can also be used for initializing the BB library
   without creating the BB. If size is zero, the library is
   initialized but no shared memory object for the BB is created.

   If the call succeeds, 0 is returned.
   If the call fails, a negative error code is returned and an
   error string can be retrieved with bb_get_error().

   Parameters:
       name    [in]  the name of the BB object
       size    [in]  size in bytes of the BB object
       dumpdir [in]  directory for the BB dump files

   Return value:
       0  if the call succeeds,
      <0  if the call fails. Possible error codes include:
          BB_ERROR_OPEN_ALREADY    the BB is already open
*/
extern int bb_open(const char *name, size_t size, const char *dumpdir);


/*
   Opens a existing BB object. This function useful for a process that
   does not write message to the BB, but reads the messages and
   disables/enables the BB. This function must be called before
   bb_get_state() and bb_set_state() calls.

   If the call succeeds, 0 is returned.
   If the call fails, a negative error code is returned and an
   error string can be retrieved with bb_get_error().

   Parameters:
       name [in]  the name of the BB object

   Return value:
       0  if the call succeeds,
      <0  if the call fails. Possible error codes include:
          BB_ERROR_OPEN_ALREADY    the BB is already open
*/
extern int bb_open_readonly(const char *name);


/*
   Writes a log message to the BB. This function is multi-thread safe.

   The keyword parameter is a (short) zero-terminated string that
   identifies the component from which the log message originated
   or the context at which the log message was generated.

   The "message" parameter is a string that does not need to be
   zero-terminated. The length of the string in bytes is given by the
   "length" parameter.

   The BB must be opened with bb_open() before calling this function.

   If the call succeeds, 0 is returned.
   If the call fails, a negative error code is returned and an
   error string can be retrieved with bb_get_error().

   Parameters:
       keyword  [in]  a string identifying the origin of the message
       message  [in]  a log message
       length   [in]  the length of the message in bytes

   Return value:
       0  if the call succeeds,
      <0  if the call fails. Possible error codes include:
          BB_ERROR_CLOSED    the BB is not open
*/
extern int bb_write(const char *keyword, const char *message, size_t length);


/*
   Performs an operation analogous to unlink(2). It deletes the name
   of the BB and, after all processes have stopped using it, the
   contents of the BB shared memory object.

   If the call fails, a negative error code is returned and an
   error string can be retrieved with bb_get_error().

   Return value:
       0  if the call succeeds,
      <0  if the call fails
 */
extern int bb_unlink();


/*
   Writes a BB dump file for the BB object with the given name.
   The BB dump file is created in the dumpdir.

   If the call succeeds, 0 is returned.

   Parameters:
       name    [in]  the name of the black box
       dumpdir [in]  directory for the dump file

   Return value:
       0  if the call succeeds,
      <0  if the call fails
*/
extern int bb_dump(const char *name, const char *dumpdir);


/*
   Return the state of the BB object (created with bb_open() call).

   If the call succeeds, a positive state code is returned.

   Return value:
     >=0  if the call succeeds,
      <0  if the call fails
*/
extern int bb_get_state();


/*
   Enable or disable the BB object (opened with bb_open() or
   bb_open_readonly()).

   Parameters:
       enable  [in]   if zero, disable the BB
                      if non-zero, enable the BB

   Return value:
       0  if the call succeeds,
      <0  if the call fails
*/
extern int bb_set_state(int enable);


/*
   Returns a pointer to a string that contains the error message
   for the latest error associated with a bb_XXX() call.

   The caller must not change the returned value.

   Parameters:
       error_code  [in]  an error code returned by a bb_XXX() call

   Return value:
       an error message corresponding to the error code
*/
extern const char *bb_get_error(int error_code);


#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* BLACKBOX_H */
