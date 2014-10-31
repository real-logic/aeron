#ifndef INCLUDED_AERON_COMMAND_CONTROLPROTOCOLEVENTS__
#define INCLUDED_AERON_COMMAND_CONTROLPROTOCOLEVENTS__

#include <cstdint>
#include <string>

namespace aeron { namespace common { namespace command {

/**
* List of event types used in the control protocol between the
* media driver and the core.
*/
struct ControlProtocolEvents
{
    // Clients to Media Driver

    /** Add Publication */
    static const std::int32_t ADD_PUBLICATION = 0x01;
    /** Remove Publication */
    static const std::int32_t REMOVE_PUBLICATION = 0x02;
    /** Add Subscriber */
    static const std::int32_t ADD_SUBSCRIPTION = 0x04;
    /** Remove Subscriber */
    static const std::int32_t REMOVE_SUBSCRIPTION = 0x05;
    /** Keepalive from Client */
    static const std::int32_t CLIENT_KEEPALIVE = 0x06;

    // Media Driver to Clients

    /** Error Response */
    static const std::int32_t ON_ERROR = 0x0F01;
    /** New subscription Buffer Notification */
    static const std::int32_t ON_CONNECTION_READY = 0x0F02;
    /** New publication Buffer Notification */
    static const std::int32_t ON_PUBLICATION_READY = 0x0F03;
    /** Operation Succeeded */
    static const std::int32_t ON_OPERATION_SUCCESS = 0x0F04;
    /** Inform client of timeout and removal of inactive connection */
    static const std::int32_t ON_INACTIVE_CONNECTION = 0x0F05;
};

}}};
#endif