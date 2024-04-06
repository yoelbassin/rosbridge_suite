# Software License Agreement (BSD License)
#
# Copyright (c) 2012, Willow Garage, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


from rclpy.node import Node

import fnmatch
from functools import partial
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Union

from rosbridge_library.protocol import Protocol
from rosbridge_library.util.models import CapabilityBaseModel
from rosbridge_library.capability import Capability
from rosbridge_library.internal.pngcompression import encode as encode_png
from rosbridge_library.internal.subscribers import manager
from rosbridge_library.internal.subscription_modifiers import MessageHandler

try:
    from ujson import dumps as encode_json
except ImportError:
    try:
        from simplejson import dumps as encode_json
    except ImportError:
        from json import dumps as encode_json


class SubscribeModel(CapabilityBaseModel):
    """
    sid             -- the subscription id from the client
    msg_type        -- the type of the message to subscribe to
    throttle_rate   -- the minimum time (in ms) allowed between messages
        being sent.  If multiple subscriptions, the lower of these is used
    queue_length    -- the number of messages that can be buffered.  If
        multiple subscriptions, the lower of these is used
    fragment_size   -- None if no fragmentation, or the maximum length of
        allowed outgoing messages
    compression     -- "none" if no compression, or some other value if
    compression is to be used (current valid values are 'png')
    """

    topic: str
    type: Optional[str] = None
    throttle_rate: Optional[int] = 0
    fragment_size: Optional[int] = None
    queue_length: Optional[int] = 0
    compression: Optional[str] = "none"


class UnsubscribeModel(CapabilityBaseModel):
    topic: str


class Subscription:
    """Keeps track of the clients multiple calls to subscribe.

    Chooses the most appropriate settings to send messages"""

    def __init__(
        self,
        client_id: Union[str, int],
        topic: str,
        publish: Callable[[Dict[str, Any], Optional[int], str], None],
        node_handle: Node,
    ) -> None:
        """Create a subscription for the specified client on the specified
        topic, with callback publish

        Keyword arguments:
        client_id -- the ID of the client making this subscription
        topic     -- the name of the topic to subscribe to
        publish   -- the callback function for incoming messages
        node_handle -- Handle to a rclpy node to create the publisher.

        """
        self.client_id = client_id
        self.topic = topic
        self.publish = publish
        self.node_handle = node_handle

        self.clients: Dict[Union[str, int, None], Any] = {}

        self.handler = MessageHandler(None, self._publish)
        self.handler_lock = Lock()

        # params
        self.throttle_rate = 0
        self.queue_length = 0
        self.fragment_size = None
        self.compression = "none"

    def unregister(self) -> None:
        """Unsubscribes this subscription and cleans up resources"""
        manager.unsubscribe(self.client_id, self.topic)
        with self.handler_lock:
            self.handler.finish(block=False)
        self.clients.clear()

    def subscribe(self, request: SubscribeModel) -> None:
        """Add another client's subscription request

        If there are multiple calls to subscribe, the values actually used for
        queue_length, fragment_size, compression and throttle_rate are
        chosen to encompass all subscriptions' requirements

        Keyword arguments:
        request         -- the subscription request received from the client

        """

        client_details = {
            "throttle_rate": request.throttle_rate,
            "queue_length": request.queue_length,
            "fragment_size": request.fragment_size,
            "compression": request.compression,
        }

        self.clients[request.sid] = client_details

        self.update_params()

        raw = request.compression == "cbor-raw"

        # Subscribe with the manager. This will propagate any exceptions
        manager.subscribe(
            self.client_id,
            self.topic,
            self.on_msg,
            self.node_handle,
            msg_type=request.type,
            raw=raw,
        )

    def unsubscribe(self, sid=None) -> None:
        """Unsubscribe this particular client's subscription

        Keyword arguments:
        sid -- the individual subscription id.  If None, all are unsubscribed

        """
        if sid is None:
            self.clients.clear()
        elif sid in self.clients:
            del self.clients[sid]

        if not self.is_empty():
            self.update_params()

    def is_empty(self) -> bool:
        """Return true if there are no subscriptions currently"""
        return len(self.clients) == 0

    def _publish(self, message) -> None:
        """Internal method to propagate published messages to the registered
        publish callback"""
        self.publish(message, self.fragment_size, self.compression)

    def on_msg(self, msg) -> None:
        """Raw callback called by subscription manager for all incoming
        messages.

        Incoming messages are passed to the message handler which may drop,
        buffer, or propagate the message

        """
        with self.handler_lock:
            self.handler.handle_message(msg)

    def update_params(self) -> None:
        """Determine the 'lowest common denominator' params to satisfy all
        subscribed clients."""

        def f(fieldname: str) -> List[Any]:
            return [x[fieldname] for x in self.clients.values()]

        self.throttle_rate = min(f("throttle_rate"))
        self.queue_length = min(f("queue_length"))
        frags = [x for x in f("fragment_size") if x is not None]
        if frags == []:
            self.fragment_size = None
        else:
            self.fragment_size = min(frags)

        self.compression = "none"
        if "png" in f("compression"):
            self.compression = "png"
        if "cbor" in f("compression"):
            self.compression = "cbor"
        if "cbor-raw" in f("compression"):
            self.compression = "cbor-raw"

        with self.handler_lock:
            self.handler = self.handler.set_throttle_rate(self.throttle_rate)
            self.handler = self.handler.set_queue_length(self.queue_length)


class Subscribe(Capability):
    topics_glob: List[str] = None

    def __init__(self, protocol: Protocol) -> None:
        # Call superclass constructor
        Capability.__init__(self, protocol)

        # Register the operations that this capability provides
        protocol.register_operation("subscribe", self.subscribe)
        protocol.register_operation("unsubscribe", self.unsubscribe)

        self._subscriptions: Dict[str, Subscription] = {}

    def subscribe(self, msg: Dict[str, Any]) -> None:
        request = SubscribeModel.model_validate(msg)

        if Subscribe.topics_glob is not None and Subscribe.topics_glob:
            if not self._check_security_glob(request.topic):
                return
        else:
            self.protocol.log(
                "debug", "No topic security glob, not checking subscription."
            )

        if request.topic not in self._subscriptions:
            client_id = self.protocol.client_id
            cb = partial(self.publish, request.topic)
            self._subscriptions[request.topic] = Subscription(
                client_id, request.topic, cb, self.protocol.node_handle
            )

        self._subscriptions[request.topic].subscribe(request)

        self.protocol.log("info", "Subscribed to %s" % request.topic)

    def unsubscribe(self, msg: Dict[str, Any]) -> None:
        request = UnsubscribeModel.model_validate(msg)

        if request.topic not in self._subscriptions:
            return
        self._subscriptions[request.topic].unsubscribe(request.sid)

        if self._subscriptions[request.topic].is_empty():
            self._subscriptions[request.topic].unregister()
            del self._subscriptions[request.topic]

        self.protocol.log("info", "Unsubscribed from %s" % request.topic)

    def publish(self, topic, message, fragment_size=None, compression="none") -> None:
        """Publish a message to the client

        Keyword arguments:
        topic   -- the topic to publish the message on
        message -- a ROS message wrapped by OutgoingMessage
        fragment_size -- (optional) fragment the serialized message into msgs
        with payloads not greater than this value
        compression   -- (optional) compress the message. valid values are
        'png' and 'none'

        """
        # TODO: fragmentation, proper ids

        outgoing_msg = {"op": "publish", "topic": topic}
        if compression == "png":
            outgoing_msg["msg"] = message.get_json_values()
            outgoing_msg_dumped = encode_json(outgoing_msg)
            outgoing_msg = {"op": "png", "data": encode_png(outgoing_msg_dumped)}
        elif compression == "cbor":
            outgoing_msg = message.get_cbor(outgoing_msg)
        elif compression == "cbor-raw":
            (secs, nsecs) = (
                self.protocol.node_handle.get_clock().now().seconds_nanoseconds()
            )
            outgoing_msg["msg"] = {
                "secs": secs,
                "nsecs": nsecs,
                "bytes": message.message,
            }
            outgoing_msg = message.get_cbor_raw(outgoing_msg)
        else:
            outgoing_msg["msg"] = message.get_json_values()

        self.protocol.send(outgoing_msg, compression=compression)

    def finish(self) -> None:
        for subscription in self._subscriptions.values():
            subscription.unregister()
        self._subscriptions.clear()
        self.protocol.unregister_operation("subscribe")
        self.protocol.unregister_operation("unsubscribe")

    def _check_security_glob(self, topic: str) -> bool:
        self.protocol.log(
            "debug", "Topic security glob enabled, checking topic: " + topic
        )
        for glob in Subscribe.topics_glob:
            if fnmatch.fnmatch(topic, glob):
                self.protocol.log(
                    "debug",
                    "Found match with glob " + glob + ", continuing subscription...",
                )
                return True
        self.protocol.log(
            "warn",
            "No match found for topic, cancelling subscription to: " + topic,
        )
        return False
