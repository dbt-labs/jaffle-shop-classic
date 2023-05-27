"""
    self_describing_json.py

    Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.

    This program is licensed to you under the Apache License Version 2.0,
    and you may not use this file except in compliance with the Apache License
    Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
    http://www.apache.org/licenses/LICENSE-2.0.

    Unless required by applicable law or agreed to in writing,
    software distributed under the Apache License Version 2.0 is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
    express or implied. See the Apache License Version 2.0 for the specific
    language governing permissions and limitations there under.

    Authors: Anuj More, Alex Dean, Fred Blundun, Anton Parkhomenko
    Copyright: Copyright (c) 2013-2014 Snowplow Analytics Ltd
    License: Apache License Version 2.0
"""


class Timestamp(object):
    def __init__(self, ts_type, value):
        """
            Construct base timestamp type

            :param ts_type: one of possible timestamp types, according to
                            tracker protocol
            :type ts_type:  ts_type
            :param value:   timestamp value in milliseconds
            :type value:    int
        """
        self.ts_type = ts_type
        self.value = value


class TrueTimestamp(Timestamp):
    def __init__(self, value):
        """
            Construct true_timestamp (ttm)

            :param value:   timestamp value in milliseconds
            :type value:    int
        """
        super(TrueTimestamp, self).__init__("ttm", value)


class DeviceTimestamp(Timestamp):
    def __init__(self, value):
        """
            Construct device_timestamp (dtm)

            :param value:   timestamp value in milliseconds
            :type value:    int
        """
        super(DeviceTimestamp, self).__init__("dtm", value)
