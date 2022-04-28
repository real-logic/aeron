#include "aeronc.h"
#include "aeronmd.h"
#include "gtest/gtest.h"

constexpr char kChannel[] = "aeron:ipc";
constexpr int kStream = 100;
constexpr uint8_t kMessage = 42;

class BlockingPublisher {
 public:
  ~BlockingPublisher() {
    if (publication)
      aeron_exclusive_publication_close(publication, nullptr, nullptr);
  }

  int Connect(aeron_t* client, const char* uri, int stream_id) {
    assert(publication == nullptr);
    aeron_async_add_exclusive_publication_t* add_publication = nullptr;
    if (aeron_async_add_exclusive_publication(&add_publication, client, uri,
                                              stream_id) < 0) {
      return -1;
    }
    do {
      if (aeron_async_add_exclusive_publication_poll(&publication,
                                                     add_publication) < 0) {
        return -1;
      }
    } while (!publication);
    return publication ? 0 : -1;
  }

  int Send(const uint8_t* buffer, size_t length) {
    int64_t result;
    do {
      result = aeron_exclusive_publication_offer(publication, buffer, length,
                                                 nullptr, nullptr);
    } while (result >= AERON_PUBLICATION_ADMIN_ACTION && result < 0);
    return result;
  }

 private:
  aeron_exclusive_publication_t* publication = nullptr;
};

class BlockingSubscriber {
 public:
  using MessageHandler =
      std::function<void(const uint8_t* buffer, size_t length)>;

  ~BlockingSubscriber() {
    assert(!pending_handler);
    if (fragment_assembler) aeron_fragment_assembler_delete(fragment_assembler);
    if (subscription) aeron_subscription_close(subscription, nullptr, nullptr);
  }

  int Connect(aeron_t* client, const char* uri, int stream_id) {
    aeron_async_add_subscription_t* add_subscription = nullptr;
    if (aeron_async_add_subscription(&add_subscription, client, uri, stream_id,
                                     nullptr, nullptr, nullptr, nullptr) < 0) {
      return -1;
    }

    do {
      if (aeron_async_add_subscription_poll(&subscription, add_subscription) <
          0) {
        return -1;
      }
    } while (!subscription);

    if (aeron_fragment_assembler_create(
            &fragment_assembler,
            [](void* clientd, const uint8_t* buffer, size_t length,
               aeron_header_t* header) {
              auto* handler = static_cast<MessageHandler*>(clientd);
              (*handler)(buffer, length);
              *handler = nullptr;
            },
            &pending_handler) < 0) {
      return -1;
    }
    return 0;
  }

  int Recv(MessageHandler handler) {
    assert(handler);
    assert(!pending_handler);
    pending_handler = std::move(handler);
    do {
      if (aeron_subscription_poll(subscription,
                                  aeron_fragment_assembler_handler,
                                  fragment_assembler, 1) < 0) {
        return -1;
      }
    } while (pending_handler);
    return 0;
  }

 private:
  aeron_subscription_t* subscription = nullptr;
  aeron_fragment_assembler_t* fragment_assembler = nullptr;
  MessageHandler pending_handler = nullptr;
};

class AeronTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Setup driver.
    ASSERT_EQ(aeron_driver_context_init(&driver_context), 0);
    ASSERT_EQ(
        aeron_driver_context_set_dir_delete_on_start(driver_context, true), 0);
    ASSERT_EQ(
        aeron_driver_context_set_dir_delete_on_shutdown(driver_context, true),
        0);
    ASSERT_EQ(aeron_driver_context_set_threading_mode(
                  driver_context, AERON_THREADING_MODE_SHARED),
              0);
    ASSERT_EQ(aeron_driver_init(&driver, driver_context), 0);
    ASSERT_EQ(aeron_driver_start(driver, false), 0);

    // Setup client.
    ASSERT_EQ(aeron_context_init(&context), 0);
    ASSERT_EQ(aeron_init(&aeron, context), 0);
    ASSERT_EQ(aeron_start(aeron), 0);
  }

  void TearDown() override {
    // Teardown client.
    aeron_close(aeron);
    aeron_context_close(context);

    // Teardown driver.
    aeron_driver_close(driver);
    aeron_driver_context_close(driver_context);
  }

  // Driver.
  aeron_driver_context_t* driver_context = nullptr;
  aeron_driver_t* driver = nullptr;
  // Client.
  aeron_context_t* context = nullptr;
  aeron_t* aeron = nullptr;
};

TEST_F(AeronTest, OneToOne) {
  BlockingSubscriber subscriber;
  ASSERT_EQ(subscriber.Connect(aeron, kChannel, kStream), 0);

  BlockingPublisher publisher;
  ASSERT_EQ(publisher.Connect(aeron, kChannel, kStream), 0);
  ASSERT_GT(publisher.Send(&kMessage, sizeof(kMessage)), 0);

  std::vector<uint8_t> message;
  auto message_handler = [&message](const uint8_t* buffer, size_t length) {
    message.assign(buffer, buffer + length);
  };
  ASSERT_EQ(subscriber.Recv(std::move(message_handler)), 0);
  ASSERT_EQ(message.size(), 1);
  EXPECT_EQ(message[0], kMessage);
}

TEST_F(AeronTest, OneToMany) {
  std::array<BlockingSubscriber, 2> subscribers;
  for (auto& subscriber : subscribers) {
    ASSERT_EQ(subscriber.Connect(aeron, kChannel, kStream), 0);
  }

  BlockingPublisher publisher;
  ASSERT_EQ(publisher.Connect(aeron, kChannel, kStream), 0);
  ASSERT_GT(publisher.Send(&kMessage, sizeof(kMessage)), 0);

  for (auto& subscriber : subscribers) {
    std::vector<uint8_t> message;
    auto message_handler = [&message](const uint8_t* buffer, size_t length) {
      message.assign(buffer, buffer + length);
    };
    ASSERT_EQ(subscriber.Recv(std::move(message_handler)), 0);
    ASSERT_EQ(message.size(), 1);
    EXPECT_EQ(message[0], kMessage);
  }
}
