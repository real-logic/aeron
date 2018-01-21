/*
 * Copyright 2018 Real Logic Ltd.
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
package io.aeron.cluster;

public class ClusterMember
{
    private boolean isLeader;
    private final int id;
    private final String clientFacingEndpoint;
    private final String memberFacingEndpoint;

    public ClusterMember(final int id, final String clientFacingEndpoint, final String memberFacingEndpoint)
    {
        this.id = id;
        this.clientFacingEndpoint = clientFacingEndpoint;
        this.memberFacingEndpoint = memberFacingEndpoint;
    }

    public void isLeader(final boolean isLeader)
    {
        this.isLeader = isLeader;
    }

    public boolean isLeader()
    {
        return isLeader;
    }

    public int id()
    {
        return id;
    }

    public String memberFacingEndpoint()
    {
        return memberFacingEndpoint;
    }

    public String clientFacingEndpoint()
    {
        return clientFacingEndpoint;
    }

    public static ClusterMember[] parse(final String value)
    {
        final String[] memberValues = value.split("\\|");
        final int length = memberValues.length;
        final ClusterMember[] members = new ClusterMember[length];

        for (int i = 0; i < length; i++)
        {
            final String[] memberAttributes = memberValues[i].split(",");
            if (memberAttributes.length != 3)
            {
                throw new IllegalStateException("Invalid member value: " + memberValues[i]);
            }

            members[i] = new ClusterMember(
                Integer.parseInt(memberAttributes[0]),
                memberAttributes[1],
                memberAttributes[2]);
        }

        return members;
    }
}
