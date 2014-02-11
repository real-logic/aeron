# Aeron Protocol Specification

## Introduction

## Design Concepts

## Channel Framing

Stylistic way of representation for doc

     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |         Length (16)           |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +-+-------------------------------------------------------------+
    |                   Frame Payload (0...)                      ...
    +---------------------------------------------------------------+

                               Frame Header

The fields of the frame header are defined as:

__Length:__  The length of the frame payload expressed as an unsigned 16-
bit integer.  The 8 octets of the frame header are not included in
this value.

__Type:__ The 8-bit type of the frame.  The frame type determines how
the remainder of the frame header and payload are interpreted.
Implementations MUST ignore unsupported and unrecognized frame
types.

