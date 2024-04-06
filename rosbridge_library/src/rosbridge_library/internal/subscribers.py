# Software License Agreement (BSD License)
#
# Copyright (c) 2012, Willow Garage, Inc.
# Copyright (c) 2013, PAL Robotics SL
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

from threading import Lock, RLock
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, Type

from rclpy.node import Node
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.subscription import Subscription
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
from rosbridge_library.internal import ros_loader
from rosbridge_library.internal.message_conversion import msg_class_type_repr
from rosbridge_library.internal.outgoing_message import OutgoingMessage
from rosbridge_library.internal.topics import (
    TopicNotEstablishedException,
    TypeConflictException,
)

""" Manages and interfaces with ROS Subscriber objects.  A single subscriber
is shared between multiple clients
"""


class SubscriptionFactory:
    def __init__(
        self,
        node_handle: Node,
        topic: str,
        msg_class: Type,
        qos: QoSProfile,
        raw: bool,
        callback_group: MutuallyExclusiveCallbackGroup,
    ) -> None:
        self._node_handle = node_handle
        self._topic = topic
        self._msg_class = msg_class
        self._qos = qos
        self._raw = raw
        self._callback_group = callback_group

    def create_subscriber(self, callback: Callable[[Any], None]) -> Subscription:
        return self._node_handle.create_subscription(
            self._msg_class,
            self._topic,
            callback,
            self._qos,
            raw=self._raw,
            callback_group=self._callback_group,
        )
        
    @staticmethod        
    def _get_default_qos(node_handle: Node, topic: str) -> QoSProfile:
        # Certain combinations of publisher and subscriber QoS parameters are
        # incompatible. Here we make a "best effort" attempt to match existing
        # publishers for the requested topic. This is not perfect because more
        # publishers may come online after our subscriber is set up, but we try
        # to provide sane defaults. For more information, see:
        # - https://docs.ros.org/en/rolling/Concepts/About-Quality-of-Service-Settings.html
        # - https://github.com/RobotWebTools/rosbridge_suite/issues/551
        qos = QoSProfile(
            depth=10,
            durability=DurabilityPolicy.VOLATILE,
            reliability=ReliabilityPolicy.RELIABLE,
        )

        infos = node_handle.get_publishers_info_by_topic(topic)
        if any(
            pub.qos_profile.durability == DurabilityPolicy.TRANSIENT_LOCAL for pub in infos
        ):
            qos.durability = DurabilityPolicy.TRANSIENT_LOCAL
        if any(
            pub.qos_profile.reliability == ReliabilityPolicy.BEST_EFFORT for pub in infos
        ):
            qos.reliability = ReliabilityPolicy.BEST_EFFORT
        return qos


    @staticmethod
    def _get_message_class(
        node_handle: Node, topic: str, msg_type: Optional[str] = None
    ) -> Type:
        # First check to see if the topic is already established
        topics_names_and_types = dict(node_handle.get_topic_names_and_types())
        topic_types = topics_names_and_types.get(topic)

        # If it's not established and no type was specified, exception
        if msg_type is None and topic_types is None:
            raise TopicNotEstablishedException(topic)

        # topic_type is a list of types or None at this point; only one type is supported.
        topic_type = None
        if topic_types is not None:
            if len(topic_types) > 1:
                node_handle.get_logger().warning(
                    f"More than one topic type detected: {topic_types}"
                )
            topic_type = topic_types[0]

        # Use the established topic type if none was specified
        if msg_type is None:
            msg_type = topic_type

        # Load the message class, propagating any exceptions from bad msg types
        msg_class = ros_loader.get_message_class(msg_type)

        # Make sure the specified msg type and established msg type are same
        msg_type_string = msg_class_type_repr(msg_class)
        if topic_type is not None and topic_type != msg_type_string:
            raise TypeConflictException(topic, topic_type, msg_type_string)
        return msg_class    

class MultiSubscriber:
    """Handles multiple clients for a single subscriber.

    Converts msgs to JSON before handing them to callbacks.  Due to subscriber
    callbacks being called in separate threads, must lock whenever modifying
    or accessing the subscribed clients."""

    def __init__(
        self,
        topic: str,
        client_id: Union[str, int],
        callback: Callable[[OutgoingMessage], None],
        node_handle: Node,
        msg_type: Optional[str] = None,
        raw: bool = False,
    ):
        """Register a subscriber on the specified topic.

        Keyword arguments:
        topic    -- the name of the topic to register the subscriber on
        client_id -- the ID of the client subscribing
        callback  -- this client's callback, that will be called for incoming
        messages
        node_handle -- Handle to a rclpy node to create the publisher.
        msg_type -- (optional) the type to register the subscriber as.  If not
        provided, an attempt will be made to infer the topic type

        Throws:
        TopicNotEstablishedException -- if no msg_type was specified by the
        caller and the topic is not yet established, so a topic type cannot
        be inferred
        TypeConflictException        -- if the msg_type was specified by the
        caller and the topic is established, and the established type is
        different to the user-specified msg_type

        """
        self._msg_class = SubscriptionFactory._get_message_class(node_handle, topic, msg_type)
        self._topic = topic

        qos = SubscriptionFactory._get_default_qos(node_handle, topic)

        self._subscriber_factory = SubscriptionFactory(
            node_handle=node_handle,
            topic=topic,
            msg_class=self._msg_class,
            qos=qos,
            raw=raw,
            callback_group=MutuallyExclusiveCallbackGroup(),
        )

        # Create the subscriber and associated member variables
        # Subscriptions is initialized with the current client to start with.
        self._subscriptions = {client_id: callback}
        self._rlock = RLock()
        self._node_handle = node_handle

        self._subscriber = self._subscriber_factory.create_subscriber(self._callback)

        self._new_subscriber: Optional[Subscription] = None
        self._new_subscriptions: Dict[
            Union[str, int], Callable[[OutgoingMessage], None]
        ] = {}

    def unregister(self):
        self._node_handle.destroy_subscription(self._subscriber)
        with self._rlock:
            self._subscriptions.clear()
            if self._new_subscriber:
                self._node_handle.destroy_subscription(self._new_subscriber)
                self._new_subscriber = None

    def verify_type(self, msg_type):
        """Verify that the subscriber subscribes to messages of this type.

        Keyword arguments:
        msg_type -- the type to check this subscriber against

        Throws:
        Exception -- if ros_loader cannot load the specified msg type
        TypeConflictException -- if the msg_type is different than the type of
        this publisher

        """
        if ros_loader.get_message_class(msg_type) is not self._msg_class:
            raise TypeConflictException(
                self._topic, msg_class_type_repr(self._msg_class), msg_type
            )

    def subscribe(self, client_id: Union[str, int], callback) -> None:
        """Subscribe the specified client to this subscriber.

        Keyword arguments:
        client_id -- the ID of the client subscribing
        callback  -- this client's callback, that will be called for incoming
        messages

        """
        with self._rlock:
            # If the topic is latched, adding a new subscriber will immediately invoke
            # the given callback.
            # In any case, the first message is handled using new_sub_callback,
            # which adds the new callback to the subscriptions dictionary.
            self._new_subscriptions.update({client_id: callback})
            if self._new_subscriber is None:
                self._new_subscriber = self._subscriber_factory.create_subscriber(
                    self._new_sub_callback
                )

    def unsubscribe(self, client_id: Union[str, int]) -> None:
        """Unsubscribe the specified client from this subscriber

        Keyword arguments:
        client_id -- the ID of the client to unsubscribe

        """
        with self._rlock:
            if client_id in self._new_subscriptions:
                del self._new_subscriptions[client_id]
            else:
                del self._subscriptions[client_id]

    def has_subscribers(self) -> bool:
        """Return true if there are subscribers"""
        with self._rlock:
            return len(self._subscriptions) + len(self._new_subscriptions) != 0

    def _callback(
        self,
        msg,
        callbacks: Optional[Iterable[Callable[[OutgoingMessage], None]]] = None,
    ):
        """Callback for incoming messages on the rclpy subscription.

        Passes the message to registered subscriber callbacks.

        Keyword Arguments:
        msg - the ROS message coming from the subscriber
        callbacks - subscriber callbacks to invoke

        """
        outgoing = OutgoingMessage(msg)

        with self._rlock:
            callbacks = callbacks or self._subscriptions.values()

            # Pass the JSON to each of the callbacks
            for callback in callbacks:
                try:
                    callback(outgoing)
                except Exception as exc:
                    # Do nothing if one particular callback fails except log it
                    self._node_handle.get_logger().error(
                        f"Exception calling subscribe callback: {exc}"
                    )

    def _new_sub_callback(self, msg):
        """
        Callbacks for new subscribers.

        If the topic was latched, a new subscriber has to be added to receive
        a new message and route it to the new subscriptor.

        After the first message is routed, the new subscriber is deleted and
        the subscriptions dictionary is updated with the newly incorporated
        subscriptors.
        """
        with self._rlock:
            self._callback(msg, self._new_subscriptions.values())
            self._subscriptions.update(self._new_subscriptions)
            self._new_subscriptions = {}
            self._node_handle.destroy_subscription(self._new_subscriber)
            self._new_subscriber = None


class SubscriberManager:
    """
    Keeps track of client subscriptions
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._subscribers: Dict[str, MultiSubscriber] = {}

    def subscribe(
        self,
        client_id: Union[str, int],
        topic: str,
        callback,
        node_handle: Node,
        msg_type: Optional[str] = None,
        raw: bool = False,
    ):
        """Subscribe to a topic

        Keyword arguments:
        client_id -- the ID of the client making this subscribe request
        topic     -- the name of the topic to subscribe to
        callback  -- the callback to call for incoming messages on the topic
        msg_type  -- (optional) the type of the topic

        """
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = MultiSubscriber(
                    topic, client_id, callback, node_handle, msg_type=msg_type, raw=raw
                )
            else:
                self._subscribers[topic].subscribe(client_id, callback)

            if msg_type is not None and not raw:
                self._subscribers[topic].verify_type(msg_type)

    def unsubscribe(self, client_id: Union[str, int], topic: str):
        """Unsubscribe from a topic

        Keyword arguments:
        client_id -- the ID of the client to unsubscribe
        topic     -- the topic to unsubscribe from

        """
        with self._lock:
            if topic not in self._subscribers:
                return

            self._subscribers[topic].unsubscribe(client_id)

            if not self._subscribers[topic].has_subscribers():
                self._subscribers[topic].unregister()
                del self._subscribers[topic]


manager = SubscriberManager()
