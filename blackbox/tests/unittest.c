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

#include <check.h>
#include <getopt.h>
#include <stdio.h>
#include <string.h>

#include "blackbox/blackbox.h"


/* Test for bb_open() */
START_TEST(test_bb_open)
{
    int rcode;

    rcode = bb_open("bb-test", 1000, ".");
    ck_assert_int_eq(rcode, 0);
}
END_TEST


/* Test for bb_open() called with invalid arguments */
START_TEST(test_bb_open_neg)
{
    int rcode;

    /* the datadir does not exist */
    rcode = bb_open("bb-test", 1000, "./invalid_datadir");
    ck_assert_int_lt(rcode, 0);

    /* a nonzero size is too small, bb_open() enlarges it to a minimum size */
    rcode = bb_open("bb-test", 3, ".");
    ck_assert_int_eq(rcode, 0);
}
END_TEST


/* Test for bb_unlink() */
START_TEST(test_bb_unlink)
{
    int rcode;

    rcode = bb_open("bb-test", 1000, ".");
    ck_assert_int_eq(rcode, 0);
    
    rcode = bb_unlink();
    ck_assert_int_eq(rcode, 0);
}
END_TEST


/* Test for bb_write() */
START_TEST(test_bb_write)
{
    int rcode, i, n = 33, error_code;
    char buffer[256];

    rcode = bb_open("bb-test", 500, ".");
    ck_assert_int_eq(rcode, 0);

    for (i = 0; i < n; i++)
    {
        error_code = 1 + i % 30;
        rcode = sprintf(buffer, "%s: error %d of %d", strerror(error_code),
                        i + 1, n);
        ck_assert_int_gt(rcode, 0);
        ck_assert_int_le(rcode, sizeof(buffer));

        rcode = bb_write("Error", buffer, strlen(buffer));
        ck_assert_int_eq(rcode, 0);
    }
}
END_TEST


/* Test for bb_dump() */
START_TEST(test_bb_dump)
{
    int rcode, i, n = 33, error_code;
    char buffer[256];

    rcode = bb_open("bb-test", 500, ".");
    ck_assert_int_eq(rcode, 0);

    for (i = 0; i < n; i++)
    {
        error_code = 1 + i % 30;
        rcode = sprintf(buffer, "%s: error %d of %d", strerror(error_code),
                        i + 1, n);
        ck_assert_int_gt(rcode, 0);
        ck_assert_int_le(rcode, sizeof(buffer));

        rcode = bb_write("Error", buffer, strlen(buffer));
        ck_assert_int_eq(rcode, 0);
    }

    rcode = bb_dump("bb-test", ".");
    ck_assert_int_eq(rcode, 0);
}
END_TEST


/* Test for bb_get_state() */
START_TEST(test_bb_get_state)
{
    int rcode;

    rcode = bb_get_state();
    ck_assert_int_eq(rcode, BB_STATE_CLOSED);

    rcode = bb_open("bb-test", 500, ".");
    ck_assert_int_eq(rcode, 0);

    rcode = bb_get_state();
    ck_assert_int_eq(rcode, BB_STATE_OPERATIONAL);
}
END_TEST


Suite *basic_suite(void)
{
    Suite *suite;
    TCase *tc_open, *tc_unlink, *tc_write, *tc_dump, *tc_get_state;

    suite = suite_create("basic");

    /* test case for bb_open() */
    tc_open = tcase_create("open");
    tcase_add_test(tc_open, test_bb_open);
    tcase_add_test(tc_open, test_bb_open_neg);
    suite_add_tcase(suite, tc_open);

    /* test case for bb_unlink() */
    tc_unlink = tcase_create("unlink");
    tcase_add_test(tc_unlink, test_bb_unlink);
    suite_add_tcase(suite, tc_unlink);
    
    /* test case for bb_get_state() */
    tc_get_state = tcase_create("get_state");
    tcase_add_test(tc_get_state, test_bb_get_state);
    suite_add_tcase(suite, tc_get_state);
    
    /* test case for bb_write() */
    tc_write = tcase_create("write");
    tcase_add_test(tc_write, test_bb_write);
    suite_add_tcase(suite, tc_write);
    
    /* test case for bb_dump() */
    tc_dump = tcase_create("dump");
    tcase_add_test(tc_dump, test_bb_dump);
    suite_add_tcase(suite, tc_dump);
    
    return suite;
}


/* A big test for bb_write() */
START_TEST(test_big_write)
{
    int rcode, i, n = 100000, error_code;
    char buffer[256];

    rcode = bb_open("bb-test", 50000, ".");
    ck_assert_int_eq(rcode, 0);

    for (i = 0; i < n; i++)
    {
        error_code = 1 + i % 30;
        rcode = sprintf(buffer, "%s: error %d of %d", strerror(error_code),
                        i + 1, n);
        ck_assert_int_gt(rcode, 0);
        ck_assert_int_le(rcode, sizeof(buffer));

        rcode = bb_write("Error", buffer, strlen(buffer));
        ck_assert_int_eq(rcode, 0);
    }
}
END_TEST


/* Test for bb_dump() */
Suite *big_suite(void)
{
    Suite *suite;
    TCase *tc_write;

    suite = suite_create("big");

    /* test case for bb_write() */
    tc_write = tcase_create("write");
    tcase_add_test(tc_write, test_big_write);
    suite_add_tcase(suite, tc_write);
    
    return suite;
}


/* Remove Black Box dump files of the form "bb-test*". */
void cleanup()
{
    int err = system("rm bb-test*");
    if (err == -1)
    {
        fprintf(stderr, "system(): could not create child process\n");
    }
    else if (err == 127)
    {
        fprintf(stderr, "system(): could not execute shell\n");
    }
    else if (err)
    {
        fprintf(stderr, "system(): child process exited with: %d\n",
                WEXITSTATUS(err));
    }
}

/* if true, cleanup after all tests have been run */
static int cleanup_flag = 1;

static struct option options[] =
{
    {"no-cleanup", no_argument, &cleanup_flag, 0},
    {0, 0, 0, 0}
};


void process_options(int *argc, char *argv[])
{
    int option_index = 0, rcode;
    
    while (1)
    {
      rcode = getopt_long(*argc, argv, "", options, &option_index);

      if (rcode == -1)
      {
          /* no more unprocessed options */
          *argc -= option_index;
          return;
      }
      
      switch (rcode)
      {
      case 0:
          /* success */
          break;
          
      case '?':
          /* option error, getopt_long() already printed an error message. */
          exit(1);
          
      default:
          exit(1);
      }
    }
}


int main(int argc, char *argv[])
{
    int n_failed;
    Suite *suite;
    SRunner *runner;

    /* process command-line options */
    process_options(&argc, argv);

    /* create "basic" test suite */
    suite = basic_suite();
    runner = srunner_create(suite);

    /* run all tests in the "basic" suite */
    srunner_run_all(runner, CK_NORMAL);
    n_failed = srunner_ntests_failed(runner);
    srunner_free(runner);

    /* create "big" test suite */
    suite = big_suite();
    runner = srunner_create(suite);

    /* run all tests in the "big" suite */
    srunner_run_all(runner, CK_NORMAL);
    n_failed = srunner_ntests_failed(runner);
    srunner_free(runner);

    if (cleanup_flag)
    {
        /* remove dump files */
        cleanup();
    }

    return (n_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
