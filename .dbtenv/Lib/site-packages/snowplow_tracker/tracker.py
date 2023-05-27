"""
    tracker.py

    Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.

    This program is licensed to you under the Apache License Version 2.0,
    and you may not use this file except in compliance with the Apache License
    Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
    http://www.apache.org/licenses/LICENSE-2.0.

    Unless required by applicable law or agreed to in writing,
    software distributed under the Apache License Version 2.0 is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
    express or implied. See the Apache License Version 2.0 for the specific
    language governing permissions and limitations there under.

    Authors: Anuj More, Alex Dean, Fred Blundun
    Copyright: Copyright (c) 2013-2014 Snowplow Analytics Ltd
    License: Apache License Version 2.0
"""

import time
import uuid
import six

from snowplow_tracker import payload, _version, SelfDescribingJson
from snowplow_tracker import subject as _subject
from snowplow_tracker.timestamp import Timestamp, TrueTimestamp, DeviceTimestamp


"""
Constants & config
"""

VERSION = "py-%s" % _version.__version__
DEFAULT_ENCODE_BASE64 = True
BASE_SCHEMA_PATH = "iglu:com.snowplowanalytics.snowplow"
SCHEMA_TAG = "jsonschema"
CONTEXT_SCHEMA = "%s/contexts/%s/1-0-1" % (BASE_SCHEMA_PATH, SCHEMA_TAG)
UNSTRUCT_EVENT_SCHEMA = "%s/unstruct_event/%s/1-0-0" % (BASE_SCHEMA_PATH, SCHEMA_TAG)

"""
Tracker class
"""


class Tracker:
    def __init__(self, emitters, subject=None,
                 namespace=None, app_id=None, encode_base64=DEFAULT_ENCODE_BASE64):
        """
            :param emitters:         Emitters to which events will be sent
            :type  emitters:         list[>0](emitter) | emitter
            :param subject:          Subject to be tracked
            :type  subject:          subject | None
            :param namespace:        Identifier for the Tracker instance
            :type  namespace:        string_or_none
            :param app_id:           Application ID
            :type  app_id:           string_or_none
            :param encode_base64:    Whether JSONs in the payload should be base-64 encoded
            :type  encode_base64:    bool
        """
        if subject is None:
            subject = _subject.Subject()

        if type(emitters) is list:
            self.emitters = emitters
        else:
            self.emitters = [emitters]

        self.subject = subject
        self.encode_base64 = encode_base64

        self.standard_nv_pairs = {
            "tv": VERSION,
            "tna": namespace,
            "aid": app_id
        }
        self.timer = None

    @staticmethod
    def get_uuid():
        """
            Set transaction ID for the payload once during the lifetime of the
            event.

            :rtype:           string
        """
        return str(uuid.uuid4())

    @staticmethod
    def get_timestamp(tstamp=None):
        """
            :param tstamp:    User-input timestamp or None
            :type  tstamp:    int | float | None
            :rtype:           int
        """
        if tstamp is None:
            return int(time.time() * 1000)
        elif isinstance(tstamp, (int, float, )):
            return int(tstamp)


    """
    Tracking methods
    """
    def track(self, pb):
        """
            Send the payload to a emitter

            :param  pb:              Payload builder
            :type   pb:              payload
            :rtype:                  tracker
        """
        for emitter in self.emitters:
            emitter.input(pb.nv_pairs)
        return self

    def complete_payload(self, pb, context, tstamp):
        """
            Called by all tracking events to add the standard name-value pairs
            to the Payload object irrespective of the tracked event.

            :param  pb:              Payload builder
            :type   pb:              payload
            :param  context:         Custom context for the event
            :type   context:         context_array | None
            :param  tstamp:          Optional user-provided timestamp for the event
            :type   tstamp:          timestamp | int | float | None
            :rtype:                  tracker
        """
        pb.add("eid", Tracker.get_uuid())

        if isinstance(tstamp, TrueTimestamp):
            pb.add("ttm", tstamp.value)
        if isinstance(tstamp, DeviceTimestamp):
            pb.add("dtm", Tracker.get_timestamp(tstamp.value))
        elif isinstance(tstamp, (int, float, type(None))):
            pb.add("dtm", Tracker.get_timestamp(tstamp))

        if context is not None:
            context_jsons = list(map(lambda c: c.to_json(), context))
            context_envelope = SelfDescribingJson(CONTEXT_SCHEMA, context_jsons).to_json()
            pb.add_json(context_envelope, self.encode_base64, "cx", "co")

        pb.add_dict(self.standard_nv_pairs)

        pb.add_dict(self.subject.standard_nv_pairs)

        return self.track(pb)

    def track_struct_event(self, category, action, label=None, property_=None, value=None,
                           context=None,
                           tstamp=None):
        """
            :param  category:       Category of the event
            :type   category:       non_empty_string
            :param  action:         The event itself
            :type   action:         non_empty_string
            :param  label:          Refer to the object the action is
                                    performed on
            :type   label:          string_or_none
            :param  property_:      Property associated with either the action
                                    or the object
            :type   property_:      string_or_none
            :param  value:          A value associated with the user action
            :type   value:          int | float | None
            :param  context:        Custom context for the event
            :type   context:        context_array | None
            :rtype:                 tracker
        """
        pb = payload.Payload()
        pb.add("e", "se")
        pb.add("se_ca", category)
        pb.add("se_ac", action)
        pb.add("se_la", label)
        pb.add("se_pr", property_)
        pb.add("se_va", value)

        return self.complete_payload(pb, context, tstamp)

    def track_unstruct_event(self, event_json, context=None, tstamp=None):
        """
            :param  event_json:      The properties of the event. Has two field:
                                     A "data" field containing the event properties and
                                     A "schema" field identifying the schema against which the data is validated
            :type   event_json:      self_describing_json
            :param  context:         Custom context for the event
            :type   context:         context_array | None
            :param  tstamp:          User-set timestamp
            :type   tstamp:          timestamp | int | None
            :rtype:                  tracker
        """

        envelope = SelfDescribingJson(UNSTRUCT_EVENT_SCHEMA, event_json.to_json()).to_json()

        pb = payload.Payload()

        pb.add("e", "ue")
        pb.add_json(envelope, self.encode_base64, "ue_px", "ue_pr")

        return self.complete_payload(pb, context, tstamp)

    # Alias
    track_self_describing_event = track_unstruct_event

    def flush(self, asynchronous=False):
        """
            Flush the emitter

            :param  asynchronous:  Whether the flush is done asynchronously. Default is False
            :type   asynchronous:  bool
            :rtype:         tracker
        """
        for emitter in self.emitters:
            if asynchronous:
                emitter.flush()
            else:
                emitter.sync_flush()
        return self

    def set_subject(self, subject):
        """
            Set the subject of the events fired by the tracker

            :param subject: Subject to be tracked
            :type  subject: subject | None
            :rtype:         tracker
        """
        self.subject = subject
        return self

    def add_emitter(self, emitter):
        """
            Add a new emitter to which events should be passed

            :param emitter: New emitter
            :type  emitter: emitter
            :rtype:         tracker
        """
        self.emitters.append(emitter)
        return self
