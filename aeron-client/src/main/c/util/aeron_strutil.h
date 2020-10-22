/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_STRUTIL_H
#define AERON_STRUTIL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>

void aeron_format_date(char *str, size_t count, int64_t timestamp);

#define AERON_FORMAT_HEX_LENGTH(b) ((2 * (b)) + 1)
void aeron_format_to_hex(char *str, size_t str_length, uint8_t *data, size_t data_len);

/*
 * FNV-1a hash function
 *
 * http://www.isthe.com/chongo/tech/comp/fnv/index.html
 */
inline uint64_t aeron_fnv_64a_buf(uint8_t *buf, size_t len)
{
    uint8_t *bp = buf;
    uint8_t *be = bp + len;
    uint64_t hval = 0xcbf29ce484222325ULL;

    while (bp < be)
    {
        hval ^= (uint64_t)*bp++;
#if defined(__GNUC__)
        hval += (hval << 1) + (hval << 4) + (hval << 5) + (hval << 7) + (hval << 8) + (hval << 40);
#else
        hval *= ((uint64_t)0x100000001b3ULL);
#endif
    }

    return hval;
}

/*
 * Splits a null terminated string using the delimiter specified, which is replaced with \0 characters.
 * Each of the tokens is stored in reverse order in the tokens array.
 *
 * Returns the number of tokens found.  Or a value < 0 for an error:
 * ERANGE: number of tokens is greater than max_tokens.
 */
int aeron_tokenise(char *input, const char delimiter, const int max_tokens, char **tokens);

/**
 * Compare two strings for equality up to the specified length. The comparison stops upon first non-equal character,
 * if the null-character is found in one of the strings or the max length reached.
 *
 * @param str1 first string to compare.
 * @param str2 second string to compare.
 * @param length number of characters to compare.
 * @return true if strings are equal up to the specified length, i.e. both strings have the same characters and are
 *         either shorter then the specified length or are same up to the length.
 */
inline bool aeron_strn_equals(const char *str1, const char *str2, const size_t length)
{
    if (str1 == str2)
    {
        return true;
    }
    else if (NULL == str1 || NULL == str2)
    {
        return false;
    }

    for (size_t i = 0; i < length; i++)
    {
        if (str1[i] != str2[i])
        {
            return false;
        }
        else if (str1[i] == '\0')
        {
           return true;
        }
    }

    return true;
}

/**
 * Compare two null-terminated strings for equality. The comparison stops upon first non-equal character found or
 * upon null-character is found in one of the strings.
 *
 * Note: if strings are not null-terminated or longer than 1024 characters the comparison will stop after 1024
 * iterations. Calling this method is equivalent of calling:
 * <pre>
 *      aeron_strn_equals(str1, str2, 1024);
 * </pre>
 *
 * @param str1 first string.
 * @param str2 second string.
 * @return true if strings are equal (or if first 1024 characters are equal in case of very long strings).
 */
inline bool aeron_str_equals(const char *str1, const char *str2)
{
    static const size_t max_length = 1024;
    assert (strlen(str1) <= max_length);
    assert (strlen(str2) <= max_length);

    return aeron_strn_equals(str1, str2, max_length);
}

#if defined(AERON_DLL_EXPORTS)
#define AERON_EXPORT __declspec(dllexport)
#else
#define AERON_EXPORT __declspec(dllimport)
#endif

#if defined(_MSC_VER) && !defined(AERON_NO_GETOPT)
AERON_EXPORT extern char *optarg;
AERON_EXPORT extern int optind;

int getopt(int argc, char *const argv[], const char *optstring);
#endif

#endif //AERON_STRUTIL_H
