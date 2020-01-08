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

#include <assert.h>
#include <errno.h>
#include <fcntl.h>           /* For O_* constants */
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/stat.h>        /* For mode constants */
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "blackbox/blackbox.h"

#ifdef __cplusplus
extern "C" {
#endif


/* the version of the Black Box memory format */
#define BB_FORMAT_VERSION 0x1

/* The maximum length of the Black Box name */
#define BB_MAX_NAME 255

/* The maximum length for the keyword parameter in a bb_write() call. */
#define BB_MAX_KEYWORD 32

/* memory alignment requirement for log the message headers */
#define BB_MESSAGE_ALIGNMENT 8

/* The minimum non-zero size for the Black Box in bb_open() call. It
   should be big enough to hold at least one non-empty message.
*/
#define BB_MIN_SIZE   256

/*
   Black box is an OS kernel object where the BB library routines save
   the log messages. It is implemented as a POSIX shared memory
   object.


   The memory layout of the shared memory objects is as follows:

   Offset                          Description
   ----------------------------------------------------------
   0 - (sizeof(bb_object_t) - 1)   The BB object struct

   sizeof(bb_object_t) - (total_size - 1 )
                                   Buffer for the log messages
   ----------------------------------------------------------
*/


/* a datatype that represents a point in time */
typedef struct timeval bb_timestamp_t;

/* a datatype representing id of a thread */
typedef pthread_t bb_thread_id_t;

/* a datatype that represents an offset of a memory location within
   a shared memory buffer. The offset is relative to the beginning of
   the buffer. The value 0 denotes a "null pointer". */
typedef size_t bb_offset_t;

/* a datatype representing other information of the BB than the log
   messages, such, as the name and size of the BB. */
typedef struct bb_metadata_struct
{
    /* version of the BB (the current version is 1) */
    uint32_t version;

    /* the state of the BB. Possible values are BB_STATE_XXX. */
    int state;

    /* the size of the BB */
    size_t size;

    /* the PID of the process that created this BB */
    int pid;

    /* the name of the BB */
    char name[BB_MAX_NAME + 1];

    /* creation time  (a zero-terminated string of the form
       "2019-08-26 13:46:52 +0300") */
#define BB_TIME_SIZE     (10 + 1 + 8 + 1 + 5)
    char creation_time[BB_TIME_SIZE + 1];

} bb_metadata_t;


/* a datatype representing a BB in a block of shared memory.
   A BB object is located at the beginning of the shared memory
   block.
*/
typedef struct bb_object_struct
{
    bb_metadata_t metadata;

    /* a linked list of log messages in time order. The newest
       message becomes the last in the list and the first message is
       removed when more space is needed.
    */
    bb_offset_t first;
    bb_offset_t last;

} bb_object_t;


/* a datatype representing the header of a log message in BB.
   The log message body follows the header.
*/
typedef struct bb_log_message_struct
{
    /* creation time of the log message */
    bb_timestamp_t timestamp;

    /* id of the thread that created the log message */
    bb_thread_id_t thread_id;

    /* the length of the log message body in bytes */
    size_t length;

    /* the next message in the list */
    bb_offset_t next;

} bb_log_message_t;


/* the name of the black box */
static char bb_name[BB_MAX_NAME + 1];

/* the address of the shared memory object */
static void *bb_addr = NULL;

/* the size in bytes of the share memory object */
static size_t bb_size = 0;

/* the location and size of the free memory in the shared memory object */
static size_t free_offset = 0;
static size_t free_size = 0;

static pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;


/*
    Create a new black box and open it for writing log messages with
    bb_write().

    Parameters:
        name       [in]  the name of the black box
        size       [in]  the size of the black box in bytes
 */
static int create_and_open(const char *name, size_t size)
{
    int bb_fd, rcode;
    size_t total_size = size + sizeof(bb_object_t);

    /* create a share memory object */
    bb_fd = shm_open(name, O_RDWR | O_CREAT, 0644);
    if (bb_fd < 0)
    {
        /* failure */
        return (-errno);
    }

    /* set the size of the shared memory object */
    rcode = ftruncate(bb_fd, total_size);
    if (rcode < 0)
    {
        /* failure */
        close(bb_fd);
        shm_unlink(name);
        return (-errno);
    }

    /* map the shared memory object into this processes address space */
    bb_addr = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                   bb_fd, 0);
    if (bb_addr == MAP_FAILED)
    {
        /* failure */
        close(bb_fd);
        bb_addr = NULL;
        shm_unlink(name);

        return (-errno);
    }
    close(bb_fd);

    /* save the name of the black box */
    strncpy(bb_name, name, sizeof(bb_name));
    bb_name[sizeof(bb_name) - 1] = 0;

    bb_size = total_size;

    /* initialize the shared memory block */
    {
        bb_object_t *bb = (bb_object_t *)bb_addr;
        struct tm timestamp_in_localtime;
        struct timeval now;
        struct timezone timezone;
        char timestamp_string[100];

        memset(bb_addr, 0, sizeof(bb_object_t));
        bb->metadata.version = BB_FORMAT_VERSION;
        bb->metadata.state = BB_STATE_OPERATIONAL;
        bb->metadata.size = size;
        strcpy(bb->metadata.name, bb_name);
        bb->metadata.pid = getpid();

        /* print timestamp */
        gettimeofday(&now, &timezone);
        localtime_r(&now.tv_sec, &timestamp_in_localtime);
        strftime(timestamp_string, sizeof(timestamp_string),
                 "%Y-%m-%d %H:%M:%S %z", &timestamp_in_localtime);
        strncpy(bb->metadata.creation_time, timestamp_string, BB_TIME_SIZE);
        bb->metadata.creation_time[BB_TIME_SIZE] = 0;

        bb->first = 0;
        bb->last = 0;
    }

    /* initially there are no messages */
    free_offset = sizeof(bb_object_t);
    free_size = size;

    /* success */
    return (0);
}


int bb_open(const char *name, size_t size, const char *dumpdir)
{
    int rcode, bb_fd;

    /* enter critical section */
    pthread_mutex_lock(&write_mutex);

    if (bb_addr != NULL)
    {
        /* there is already a handle to the black box */
        pthread_mutex_unlock(&write_mutex);
        return (-BB_ERROR_OPEN_ALREADY);
    }

    if (size == 0)
    {
        /* ok, this call just initializes the BB library without creating
           a BB. We are done here. */
        pthread_mutex_unlock(&write_mutex);
        return (0);
    }

    if (size < BB_MIN_SIZE)
    {
      /* size is too small, replace it with the minimum size */
      size = BB_MIN_SIZE;
    }

    /* check if the black box already exists */
    bb_fd = shm_open(name, O_RDONLY, 0);
    if (bb_fd >= 0)
    {
        /* the black box with this name already exists, dump and delete it */
        close(bb_fd);
        rcode = bb_dump(name, dumpdir);
        if (rcode < 0)
        {
            /* failure */
            pthread_mutex_unlock(&write_mutex);
            return (rcode);
        }

        rcode = shm_unlink(name);
        if (rcode < 0)
        {
            /* failure */
            pthread_mutex_unlock(&write_mutex);
            return (-errno);
        }
    }
    else
    {
        /* failure */
        if (errno != ENOENT)
        {
            /* failed because of something else than non-existence */
            pthread_mutex_unlock(&write_mutex);
            return (-errno);
        }
    }

    /* ok, the black box does not exist, create it and set its size */
    rcode = create_and_open(name, size);

    /* leave critical section */
    pthread_mutex_unlock(&write_mutex);

    return (rcode);
}


/*
   Check the Black Box object in the shared memory block.

   Parameters:
       bb  [in]  a Black Box object

   Return value:
       0  if the metadata is good
      <0  otherwise
 */
static int check_metadata(bb_object_t *bb)
{
    assert(bb);

    if (bb->metadata.version != BB_FORMAT_VERSION)
    {
        /* bad version */
        return (-BB_ERROR_FORMAT);
    }
    if (!(bb->metadata.state == BB_STATE_CLOSED ||
          bb->metadata.state == BB_STATE_DISABLED ||
          bb->metadata.state == BB_STATE_OPERATIONAL))
    {
        /* bad state */
        return (-BB_ERROR_FORMAT);
    }

    return (0);
}


int bb_open_readonly(const char *name)
{
    int rcode, bb_fd;
    struct stat info;
    size_t total_size;
    bb_object_t *bb = (bb_object_t *)bb_addr;

    if (bb_addr != NULL)
    {
        /* there is already a handle to the black box */
        return (-BB_ERROR_OPEN_ALREADY);
    }

    /* open a handle to an existing shared memory object */
    bb_fd = shm_open(name, O_RDWR, 0);
    if (bb_fd < 0)
    {
        /* failure */
        return (-errno);
    }

    /* ok, the black box exists */

    /* get the size of the shared memory block */
    rcode = fstat(bb_fd, &info);
    if (rcode < 0)
    {
        /* failure */
        close(bb_fd);
        bb_fd = -1;
        return (rcode);
    }
    if (info.st_size < (off_t)sizeof(bb_object_t))
    {
        /* failure: the shared memory block is too small */
        close(bb_fd);
        return (-BB_ERROR_SIZE);
    }
    total_size = info.st_size;

    /* map the shared memory into this processes address space */
    bb_addr = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                   bb_fd, 0);
    if (bb_addr == MAP_FAILED)
    {
        /* failure */
        close(bb_fd);
        bb_addr = NULL;

        return (-errno);
    }
    bb = (bb_object_t *)bb_addr;
    close(bb_fd);

    rcode = check_metadata(bb);
    if (rcode < 0)
    {
        /* failure */
        munmap(bb_addr, total_size);
        bb_addr = NULL;
        return (rcode);
    }

    /* save the name of the black box */
    strncpy(bb_name, name, sizeof(bb_name));
    bb_name[sizeof(bb_name) - 1] = 0;

    bb_size = total_size;

    /* success */
    return (0);
}


/*
   Return the pointer corresponding to the offset of a log message
   in a shared memory block.

   Parameters:
       block   [in]  a shared memory block
       offset  [in]  offset of a log message header relative to the
                     beginning of the shared memory block, or 0

   Return value:
       a pointer to log message header or a NULL pointer
 */
static inline bb_log_message_t *message_ptr2(void *block, bb_offset_t offset)
{
    if (offset)
    {
        return (bb_log_message_t *)(block + offset);
    }
    else
    {
        return (NULL);
    }
}


/*
   Return the pointer corresponding to the offset of a log message
   in the shared memory block.

   Parameters:
       offset  [in]  offset of a log message header relative to the
                     beginning of the shared memory block, or 0

   Return value:
       a pointer to log message header or a NULL pointer
 */
static inline bb_log_message_t *message_ptr(bb_offset_t offset)
{
    return (message_ptr2(bb_addr, offset));
}


/*
   Round up the offset to the next multiple of alignment.

   Parameters:
       offset     [in]  a non-negative integer
       alignment  [in]  a positive integer

   Return value:
       the smallest multiple of alignment that is not smaller than
       the offset
 */
static inline size_t align_up(size_t offset, size_t alignment)
{
    return (offset + (alignment - 1)) & ~(alignment - 1);
}


/*
   Check that the free space is big enough a log message of the given
   size. If the free space is too small, delete oldest messages until
   there is sufficient free space.

   Parameters:
       message_size [in]  total size of the log message in bytes

   Return value:
       1  if the call succeeds
       0  otherwise
*/
static inline int request_free_space(size_t message_size)
{
    bb_object_t *bb = (bb_object_t *)bb_addr;
    size_t total_size = bb->metadata.size + sizeof(bb_object_t);

    /* find free space for the message */
    while (free_size < message_size && bb->first)
    {
        if (free_offset > bb->first) {
            /* free block follows the last message in the list:

               |-First -- LIST -- Last | _ _ _ Free _ _ _ |

               but the free block at the end is too small,
               move the free block to the beginning off the buffer.
            */
            free_offset = sizeof(bb_object_t);
            free_size = bb->first - free_offset;
        }
        else
        {
            /* The free block is before the first message, enlarge the free
               block by removing the first message
             */
            bb_log_message_t *first;

            first = message_ptr(bb->first);
            if (first->next)
            {
                if (first->next > bb->first)
                {
                    free_size += first->next - bb->first;
                }
                else
                {
                    free_size += total_size - bb->first;
                }

                /* delete the first element from the list */
                bb->first = first->next;
            }
            else
            {
                /* only one elment in the list, remove it */
                assert(bb->first == bb->last);

                bb->first = 0;
                bb->last = 0;
                free_offset = sizeof(bb_object_t);
                free_size = total_size - free_offset;
            }
        }
    }

    return (free_size >= message_size);
}


/*
   Appends an empty message to the list of messages in the black box.
   The message header and body are not initialized, except for the "next"
   pointer in the message header.

   Parameters:
       message_size  [in]   total size in bytes of the message

   Return value:
       a pointer to the message if call succeeds, or
       a NULL pointer if the call fails
*/
static inline bb_log_message_t *append_empty_message(size_t message_size)
{
    bb_object_t *bb = (bb_object_t *)bb_addr;
    size_t message_size_aligned = align_up(message_size, BB_MESSAGE_ALIGNMENT);
    bb_log_message_t *message;
    size_t message_offset;
    int success;

    success = request_free_space(message_size_aligned);
    if (!success)
    {
        /* failure */
        return (NULL);
    }

    assert(free_offset % BB_MESSAGE_ALIGNMENT == 0);
    assert(message_size_aligned <= free_size);

    message_offset = free_offset;
    message = message_ptr(message_offset);
    free_size -= message_size_aligned;
    free_offset += message_size_aligned;

    /* append message to the list */
    if (bb->last)
    {
        bb_log_message_t *last = message_ptr(bb->last);

        last->next = message_offset;
        message->next = 0;
        bb->last = message_offset;
    }
    else
    {
        bb->first = message_offset;
        bb->last = message_offset;
    }

    return (message);
}


int bb_write(const char *keyword, const char *message, size_t length)
{
    size_t message_size, keyword_size = 0, offset;
    bb_object_t *bb = (bb_object_t *)bb_addr;
    bb_log_message_t *header;
    char *body;

    /* enter critical section */
    pthread_mutex_lock(&write_mutex);

    if (!bb)
    {
        /* failure: BB does not exist */
        pthread_mutex_unlock(&write_mutex);
        return (-BB_ERROR_CLOSED);
    }

    if (bb->metadata.state != BB_STATE_OPERATIONAL)
    {
        /* BB is disabled */
        pthread_mutex_unlock(&write_mutex);
        return (0);
    }

    /* ok, BB is operational */

    message_size = sizeof(bb_log_message_t) + length;
    if (keyword)
    {
        /* prefix message with "[keyword] " */
        keyword_size = strlen(keyword);
        if (keyword_size > BB_MAX_KEYWORD)
        {
            keyword_size = BB_MAX_KEYWORD;
        }

        message_size += keyword_size + 3;
    }

    if (message_size > bb->metadata.size)
    {
        /* message is too long, truncate it */
        message_size = bb->metadata.size;
    }

    /* append an empty message to the BB ring buffer */
    header = append_empty_message(message_size);
    if (!header)
    {
      /* failure: this should never happen, disable BB */
      bb->metadata.state = BB_STATE_DISABLED;
      pthread_mutex_unlock(&write_mutex);
      return (-BB_ERROR_INTERNAL);
    }

    /* set message header */
    gettimeofday(&header->timestamp, NULL);
    header->thread_id = pthread_self();
    header->length = message_size - sizeof(bb_log_message_t);
    header->next = 0;

    /* set message body */
    body = (char *)header + sizeof(bb_log_message_t);
    offset = 0;
    if (keyword)
    {
        body[0] = '[';
        memcpy(body + 1, keyword, keyword_size);
        body[1 + keyword_size] = ']';
        body[2 + keyword_size] = ' ';

        offset = keyword_size + 3;
    }
    memcpy(body + offset, message,
           message_size - offset - sizeof(bb_log_message_t));

    /* leave critical section */
    pthread_mutex_unlock(&write_mutex);

    /* success */
    return (0);
}


int bb_unlink()
{
    int rcode;

    /* enter critical section */
    pthread_mutex_lock(&write_mutex);

    if (bb_addr == NULL)
    {
        /* the black box is closed */
        pthread_mutex_unlock(&write_mutex);
        return (-BB_ERROR_CLOSED);
    }

    /* unmap shared memory in this process address space */
    munmap(bb_addr, bb_size);
    bb_addr = NULL;

    /* remove shared memory object of the black box */
    rcode = shm_unlink(bb_name);

    /* leave critical section */
    pthread_mutex_unlock(&write_mutex);

    return (rcode != 0 ? -errno: 0);
}


/*
   Check that the given offset is valid and there is a valid message
   header at the offset in the shared memory block.

   Parameters:
       message_offset  [in]  a non-negative integer
       memory_size     [in]  size in bytes of the shared memory block

   Return value:
      0  if the message header at the offset is valid
*/
static int check_message(size_t message_offset, size_t memory_size, void *addr)
{
    bb_log_message_t *message;

    if (!(message_offset >= sizeof(bb_object_t) &&
          message_offset < memory_size &&
          message_offset % BB_MESSAGE_ALIGNMENT == 0))
    {
        /* failure */
        return (-BB_ERROR_FORMAT);
    }
    message = message_ptr2(addr, message_offset);
    if (!(sizeof(bb_log_message_t) + message->length < memory_size))
    {
        /* failure */
        return (-BB_ERROR_FORMAT);
    }

    /* success */
    return (0);
}


/*
   Print a message to file stream.

   Parameters:
       file     [in]  an open file stream
       message  [in]  a log message

   Return value:
       0  if call succeeds
      <0  otherwise
 */
static int print_message(FILE *file, bb_log_message_t *message)
{
    struct tm timestamp_in_localtime;
    char timestamp_string[100];
    int rcode, rcode2;

    /* print timestamp */
    localtime_r(&message->timestamp.tv_sec, &timestamp_in_localtime);
    rcode = strftime(timestamp_string, sizeof(timestamp_string),
                     "%b %d %H:%M:%S", &timestamp_in_localtime);
    if (rcode > 0)
    {
        /* print microseconds */
        rcode2 = sprintf(&timestamp_string[rcode], ".%06ld ",
                         message->timestamp.tv_usec);
        if (rcode2 > 0)
        {
            rcode += rcode2;
        }
    }

    fwrite(timestamp_string, 1, rcode, file);

    /* print thread id */
    fprintf(file, "[Thread 0x%lx] ", message->thread_id);

    /* print message */
    fwrite(&message[1], 1, message->length, file);
    fprintf(file, "\n");

    /* success */
    return (0);
}


/*
    Create a Black Box dump file and open it. If a file
    exists already, overwrite it.

    The name of the file is of the form

      <BB name>-<PID>-<TIMESTAMP>

    Parameters:
        bb_name    [in]  Black Box name
        dir_path   [in]  directory where the file is created
        pid        [in]  a process ID
        dump_file [out]  a pointer to variable where a handle
                         to the opened file is returned

    Return value:
         0  if call succeeds
        <0  otherwise
 */
static int open_dump_file(const char *bb_name,
                          const char *dir_path,
                          uint pid,
                          FILE **dump_file)
{
    char dump_file_pathname[PATH_MAX + 1];
    char timestamp_string[100];
    struct timeval now;
    struct tm timestamp_in_localtime;

    /* create timestamp string */
    gettimeofday(&now, NULL);
    localtime_r(&now.tv_sec, &timestamp_in_localtime);
    strftime(timestamp_string, sizeof(timestamp_string),
             "%Y-%m-%dT%H-%M-%S", &timestamp_in_localtime);

    sprintf(dump_file_pathname, "%s/%s_%u_%s", dir_path, bb_name, pid,
            timestamp_string);
    assert(strlen(dump_file_pathname) < sizeof(dump_file_pathname));

    *dump_file = fopen(dump_file_pathname, "w");

    if (*dump_file == NULL)
    {
        /* failure */
        return (-errno);
    }

    /* success */
    return (0);
}


int bb_dump(const char *name, const char *dumpdir)
{
    int rcode, bb_fd;
    FILE *dump_file;
    struct stat info;
    size_t total_size, message_offset;
    bb_object_t *bb;
    void *addr;

    /* open a file descriptor for the shared memory block */
    bb_fd = shm_open(name, O_RDONLY, 0);
    if (bb_fd < 0)
    {
        /* failure */
        return (-errno);
    }

    /* ok, we have the an file descriptor for the shared memory block */

    /* get the size of the shared memory block */
    rcode = fstat(bb_fd, &info);
    if (rcode < 0)
    {
        /* failure */
        close(bb_fd);
        return (-errno);
    }
    if (info.st_size < (off_t)sizeof(bb_object_t))
    {
        /* failure: the shared memory block is too small */
        close(bb_fd);
        return (-BB_ERROR_SIZE);
    }
    total_size = info.st_size;

    /* map the shared memory into this processes address space */
    addr = mmap(NULL, total_size, PROT_READ, MAP_SHARED, bb_fd, 0);
    if (addr == MAP_FAILED)
    {
        /* failure */
        close(bb_fd);

        return (-errno);
    }
    bb = (bb_object_t *)addr;
    close(bb_fd);

    rcode = check_metadata(bb);
    if (rcode < 0)
    {
        /* failure */
        munmap(addr, total_size);
        return (rcode);
    }

    if (dumpdir)
    {
        /* create the dump file, overwrite possible existing file */
        rcode = open_dump_file(name, dumpdir, bb->metadata.pid, &dump_file);
        if (rcode < 0)
        {
            /* failure */
            munmap(addr, total_size);
            return (rcode);
        }
    }
    else
    {
        /* write dump to stdout */
        dump_file = stdout;
    }

    /* print metadata */
    fprintf(dump_file, "Black Box dump file:\n"
            "    BB name = %s\n"
            "    Creation time = %s\n"
            "    Creator PID = %u\n",
            bb->metadata.name,
            bb->metadata.creation_time,
            bb->metadata.pid
            );

    /* print log messages */
    message_offset = bb->first;
    while (message_offset)
    {
        bb_log_message_t *message;

        rcode = check_message(message_offset, total_size, addr);
        if (rcode < 0) {
            /* failure */
            munmap(addr, total_size);
            if (dump_file != stdout)
            {
                fclose(dump_file);
            }
            return (rcode);
        }
        message = message_ptr2(addr, message_offset);

        /* print this message */
        print_message(dump_file, message);

        /* get the next message */
        message_offset = message->next;
    }

    munmap(addr, total_size);

    if (dump_file != stdout)
    {
        fclose(dump_file);
    }

    /* success */
    return (0);
}


int bb_get_state()
{
    bb_object_t *bb = (bb_object_t *)bb_addr;

    if (!bb)
    {
        return (BB_STATE_CLOSED);
    }

    return (bb->metadata.state);
}


int bb_set_state(int enable)
{
    bb_object_t *bb = (bb_object_t *)bb_addr;

    if (!bb)
    {
        /* failure */
        return (-BB_ERROR_CLOSED);
    }

    if (enable)
    {
        bb->metadata.state = BB_STATE_OPERATIONAL;
    }
    else
    {
        bb->metadata.state = BB_STATE_DISABLED;
    }

    /* success */
    return (0);
}


const char *bb_get_error(int error_code)
{
    const char *error;

    if (error_code < 0)
    {
        error_code = -error_code;
    }

    switch (error_code)
    {
    case BB_ERROR_SIZE:
        error = "Invalid Black Box size in bb_open()";
        break;

    case BB_ERROR_FORMAT:
        error = "Bad format of Black Box in shared memory";
        break;

    case BB_ERROR_OPEN_ALREADY:
        error = "Black Box is already open";
        break;

    case BB_ERROR_INTERNAL:
        error = "Internal Black Box error";
        break;

    default:
        error = strerror(error_code);
    }

    return (error);
}

#ifdef __cplusplus
} /* extern "C" */
#endif
