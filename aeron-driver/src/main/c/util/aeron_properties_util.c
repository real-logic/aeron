/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <errno.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include "aeron_properties_util.h"
#include "aeron_error.h"

int aeron_next_non_whitespace(const char *buffer, size_t start, size_t end)
{
    for (size_t i = start; i <= end; i++)
    {
        const char c = buffer[i];

        if (c == ' ' || c == '\t')
        {
            continue;
        }

        return (c == '\0') ? -1 : i;
    }

    return -1;
}

/*
 * Format taken from
 * https://docs.oracle.com/cd/E23095_01/Platform.93/ATGProgGuide/html/s0204propertiesfileformat01.html
 */
int aeron_properties_parse_line(
    aeron_properties_parser_state_t *state,
    const char *line,
    size_t length,
    aeron_properties_file_handler_func_t handler,
    void *clientd)
{
    bool in_name = (0 < state->name_end) ? false : true;
    int value_start = 0, result = 0;

    if (length >= (sizeof(state->property_str) - state->value_end))
    {
        aeron_set_err(EINVAL, "line length %" PRIu64 " too long for parser state", length + state->value_end);
        return -1;
    }

    if (in_name)
    {
        int cursor = aeron_next_non_whitespace(line, 0, length - 1);

        if (-1 == cursor || '!' == line[cursor] || '#' == line[cursor])
        {
            return 0;
        }

        for (size_t i = cursor; i < length; i++)
        {
            const char c = line[i];

            if (':' == c || '=' == c)
            {
                state->property_str[state->name_end] = '\0';
                value_start = i + 1;

                /* trium back for whitespace after name */
                for (size_t j = i - 1; j >= 0; j--)
                {
                    if (' ' != line[j] && '\t' != line[j])
                    {
                        break;
                    }

                    state->property_str[--state->name_end] = '\0';
                }

                state->value_end = state->name_end + 1;
                break;
            }

            state->property_str[state->name_end++] = c;
        }

        if (0 == state->value_end || 0 == state->name_end)
        {
            aeron_set_err(EINVAL, "%s", "malformed line");
            aeron_properties_parse_init(state);
            return -1;
        }

        value_start = aeron_next_non_whitespace(line, value_start, length - 1);

        if (-1 == value_start)
        {
            state->property_str[state->value_end++] = '\0';

            result = handler(clientd, state->property_str, state->property_str + state->name_end + 1);

            aeron_properties_parse_init(state);
            return result;
        }
    }
    else
    {
        value_start = aeron_next_non_whitespace(line, value_start, length - 1);

        if (-1 == value_start || '!' == line[value_start] || '#' == line[value_start])
        {
            return 0;
        }
    }

    if ('\\' == line[length - 1])
    {
        memcpy(state->property_str + state->value_end, line + value_start, length - value_start - 1);
        state->value_end += length - value_start - 1;
    }
    else
    {
        memcpy(state->property_str + state->value_end, line + value_start, length - value_start);
        state->value_end += length - value_start;
        state->property_str[state->value_end++] = '\0';

        result = handler(clientd, state->property_str, state->property_str + state->name_end + 1);

        aeron_properties_parse_init(state);
    }

    return result;
}

extern void aeron_properties_parse_init(aeron_properties_parser_state_t *state);
