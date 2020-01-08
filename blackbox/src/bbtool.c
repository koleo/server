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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "blackbox/blackbox.h"


/*
   Write the contents of the BB to stdout.

   Parameters:
        bb_name  [in]  name of a the Black Box

    Return value:
        0  if call succeeds
       <0  otherwise
*/
static int dump(const char *bb_name)
{
    int rcode;

    rcode = bb_dump(bb_name, NULL);
    if (rcode != 0)
    {
        /* failure */
        fprintf(stderr, "Failed to dump the contents of \"%s\": %s\n",
                bb_name, bb_get_error(rcode));
    }

    return (rcode);
}


/*
    Delete the Black Box. This function deletes the shared
    memory object that represents the Black Box.

    Parameters:
        bb_name  [in]  name of a the Black Box

    Return value:
        0  if call succeeds
       <0  otherwise
*/
static int delete(const char *bb_name)
{
    int rcode;

    rcode = bb_unlink(bb_name);
    if (rcode != 0)
    {
        /* failure */
        fprintf(stderr, "Failed to delete shared memory object \"%s\": %s\n",
                bb_name, bb_get_error(rcode));
    }

    return (rcode);
}


/*
    Print to the status of the Black Box to stdout.

    Parameters:
        bb_name  [in]  the name of the Black Box

    Return value:
        0  if call succeeds
       <0  otherwise
 */
static int status(const char *bb_name)
{
    int state, rcode;

    rcode = bb_open_readonly(bb_name);
    if (rcode < 0)
    {
        /* failure */
        fprintf(stderr, "Failed to open shared memory object \"%s\": %s\n",
                bb_name, bb_get_error(rcode));
        return (rcode);
    }

    state = bb_get_state();
    switch (state) {

    case BB_STATE_CLOSED:
        printf("%s is closed\n", bb_name);
        break;

    case BB_STATE_DISABLED:
        printf("%s is disabled\n", bb_name);
        break;

    case BB_STATE_OPERATIONAL:
        printf("%s is operational\n", bb_name);
        break;

    default:
        printf("Bad BB state %d\n", state);
    }

    /* success */
    return (0);
}


/*
  Enable or disable a BB.

  Parameters:
      bb_name  [in]  the name of the Black Box
      enable   [in]  if non-zero, enable the BB
                     if nero, disable the BB

  Return values:
      0  if call succeeds
     <0  otherwise
*/
static int set_status(const char *bb_name, int enable)
{
    int rcode;

    rcode = bb_open_readonly(bb_name);
    if (rcode < 0)
    {
        /* failure */
        fprintf(stderr, "Failed to open shared memory object \"%s\": %s\n",
                bb_name, bb_get_error(rcode));
        return (rcode);
    }

    rcode = bb_set_state(enable);
    if (rcode < 0)
    {
        /* failure */
        fprintf(stderr, "Failed to enable \"%s\": %s\n",
                bb_name, bb_get_error(rcode));
        return (rcode);
    }

    /* success */
    return (0);
}


/* Print help text to stdout. */
static void print_usage()
{
    printf("\nUsage: bbtool <BB name> <COMMAND>\n\n"
           "where <BB name> is the name of a Black Box\n"
           "and <COMMAND> is one of the following:\n\n"
           "  * dump    - print to stdout BB contents\n"
           "  * delete  - remove active BB\n"
           "  * status  - report the status of the BB to stdout\n"
           "  * enable  - request BB to enter operational state\n"
           "  * disable - request BB to enter disabled state\n");
}


int main(int argc, char *argv[])
{
    const char *bb_name, *command;

    /* check command line */
    if (argc > 1 && 0 == strcmp("--help", argv[1]))
    {
        print_usage();
        return (0);
    }
    if (argc != 3)
    {
        fprintf(stderr, "Invalid number of arguments to bbtool;"
                " run \"bbtool --help\" for usage.\n");
        exit(1);
    }

    /* ok, we have good command line parameters */
    bb_name = argv[1];
    command = argv[2];

    if (0 == strcmp("dump", command))
    {
        return (dump(bb_name));
    }
    else if (0 == strcmp("delete", command))
    {
        return (delete(bb_name));
    }
    else if (0 == strcmp("status", command))
    {
        return (status(bb_name));
    }
    else if (0 == strcmp("enable", command))
    {
        return (set_status(bb_name, 1));
    }
    else if (0 == strcmp("disable", command))
    {
        return (set_status(bb_name, 0));
    }
    else
    {
        fprintf(stderr, "Bad command \"%s\".\n"
                "You can print usage with --help.\n", command);
        exit(1);
    }

    /* not reached */
    return (0);
}
