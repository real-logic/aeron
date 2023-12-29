#include <stdlib.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <inttypes.h>
#include <string.h>

#include "aeronc.h"
#include "concurrent/aeron_atomic.h"
#include "util/aeron_strutil.h"
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"

#include "samples_configuration.h"

#include <pthread.h>
#include <unistd.h>
#include <assert.h>

volatile bool running = true;
bool is_running(void)
{
    bool result;
    AERON_GET_VOLATILE(result, running);
    return result;
}

typedef struct {
  int id;          // subscriber id
  aeron_t* client; // aeron client
  double snooze;   // for how long should we sleep, in seconds?
} subscriber;

void poll_handler(void* clientd, const uint8_t* buffer, size_t length, aeron_header_t* _header)
{
  subscriber* s = (subscriber*)clientd;
  uint64_t i = *(uint64_t *) buffer;
  printf("recvd[%d] %llu\n", s->id, i);
}

void* subscribe( void* arg ) {
  subscriber* s = (subscriber*)arg;

  aeron_async_add_subscription_t* sub = NULL;
  aeron_subscription_t* subscription = NULL;
  aeron_fragment_assembler_t *fragment_assembler = NULL;

  useconds_t sleep_duration_micros = (useconds_t)(s->snooze * 1e6);

  if ( aeron_async_add_subscription(
				    &sub,
				    s->client,
				    DEFAULT_CHANNEL,
				    DEFAULT_STREAM_ID,
				    NULL,
				    NULL,
				    NULL,
				    NULL
				    ) < 0 ) {
    printf("fail: aeron_async_add_subscription\n");
    goto cleanup;
  }
  
  while (NULL == subscription) {
    if (aeron_async_add_subscription_poll(&subscription, sub) < 0) {
      printf("fail: aeron_async_add_subscription_poll\n");
      goto cleanup;
    }

    if (!is_running())  {
      printf("fail: not running\n");
      goto cleanup;
    }
    sched_yield();
  }

  while (!aeron_subscription_is_connected(subscription)) {
    if (!is_running()) {
      printf("fail: not running\n");
      goto cleanup;
    }
    sched_yield();
  }

  // poll_handler will receive context s as its first argument
  if (aeron_fragment_assembler_create(&fragment_assembler, poll_handler, s) < 0) {
    printf("fail: aeron_fragment_assembler_create\n");
    goto cleanup;
  }

  while (is_running()) {
    int fragments_read = aeron_subscription_poll(
						 subscription,
						 aeron_fragment_assembler_handler,
						 fragment_assembler,
						 1 /* fragment limit */);

    if (fragments_read < 0) {
      printf("fail: aeron_subscription_poll\n");
      goto cleanup;
    }

    // sleep for a bit
    usleep( sleep_duration_micros );
  }

  cleanup:
    aeron_subscription_close(subscription, NULL, NULL);
    aeron_fragment_assembler_delete(fragment_assembler);

  pthread_exit(NULL);
}

int main(int argc, char** argv)
{
  aeron_context_t* context = NULL;
  aeron_t* aeron = NULL;

  pthread_t th1;
  pthread_t th2;

  void* th1_result;
  void* th2_result;

  subscriber sub1;
  subscriber sub2;

  if ( aeron_context_init(&context) < 0 ) {
    printf("fail: aeron_context_init\n");
    exit(1);
  }

  if ( aeron_init(&aeron, context) < 0 ) {
    printf("fail: aeron_init\n");
    exit(1);
  };

  if ( aeron_start(aeron) < 0 ) {
    printf("fail: aeron_start\n");
    exit(1);
  }

  // first subscriber
  sub1.id = 1;
  sub1.client = aeron;
  sub1.snooze = 0.050;

  // second subscriber
  sub2.id = 2;
  sub2.client = aeron;
  sub2.snooze = 0.400;

  pthread_create( &th1, NULL, subscribe, &sub1 );
  pthread_create( &th2, NULL, subscribe, &sub2 );

  pthread_join( th1, &th1_result );
  pthread_join( th2, &th2_result );

  aeron_close(aeron);
  aeron_context_close(context);

  printf("done!\n");
  return 0;
}
