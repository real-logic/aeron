/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

#ifndef AERON_AERONMD_H
#define AERON_AERONMD_H

#define AERON_MAX_PATH (256)

typedef enum aeron_threading_mode_enum
{
    AERON_THREADING_MODE_DEDICATED,
    AERON_THREADING_MODE_SHARED_NETWORK,
    AERON_THREADING_MODE_SHARED
}
aeron_threading_mode_t;

typedef struct aeron_driver_context_stct
{
    char *aeron_dir;  /* aeron.dir */
    aeron_threading_mode_t threading_mode;  /* aeron.threading.mode */
}
aeron_driver_context_t;

typedef struct aeron_driver_stct
{
    aeron_driver_context_t *context;
}
aeron_driver_t;

/* load settings from Java properties file (https://en.wikipedia.org/wiki/.properties) and set env vars */
int aeron_driver_load_properties_file(const char *filename);

/* init defaults and from env vars */
int aeron_driver_context_init(aeron_driver_context_t **context);
int aeron_driver_context_close(aeron_driver_context_t *context);

/* init driver from context */
int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context);
int aeron_driver_start(aeron_driver_t *driver);
int aeron_driver_close(aeron_driver_t *driver);

#endif //AERON_AERONMD_H
