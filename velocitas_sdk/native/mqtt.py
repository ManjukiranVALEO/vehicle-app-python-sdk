# Copyright (c) 2022-2024 Contributors to the Eclipse Foundation
#
# This program and the accompanying materials are made available under the
# terms of the Apache License, Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
import socks
from typing import Optional

import paho.mqtt.client as mqtt  # type: ignore

from velocitas_sdk.base import PubSubClient

logger = logging.getLogger(__name__)


class MqttTopicSubscription:
    """Mqtt topic subscription object that consists of topic and callback."""

    def __init__(self, topic, callback):
        self.topic = topic
        self.callback = callback


class MqttClient(PubSubClient):
    """This class is a wrapper for the on_message callback of the MQTT broker."""

    def __init__(self, hostname: str, port: Optional[int] = None, cacert: Optional[str] = None, key: Optional[str] = None, device_cert: Optional[str] = None, proxy_hostname: Optional[str] = None, proxy_port: Optional[str] = None):
        self._port = port
        self._hostname = hostname
        self._topics_to_subscribe: list[MqttTopicSubscription] = []
        self._cacert = cacert
        self._key = key
        self._device_cert = device_cert
        self._proxy_hostname = proxy_hostname
        self._proxy_port = proxy_port
        self._pub_client = mqtt.Client()
        self._sub_client = mqtt.Client()
        if self._proxy_hostname is not None and self._proxy_port is not None:
            self._pub_client.proxy_set(proxy_type=socks.HTTP, proxy_addr=self._proxy_hostname, proxy_port=self._proxy_port)
            self._sub_client.proxy_set(proxy_type=socks.HTTP, proxy_addr=self._proxy_hostname, proxy_port=self._proxy_port)
        if self._cacert is not None and self._key is not None and self._device_cert is not None:
            self._pub_client.tls_set(ca_certs=self._cacert, certfile=self._device_cert, keyfile=self._key)
            self._sub_client.tls_set(ca_certs=self._cacert, certfile=self._device_cert, keyfile=self._key)
        self._sub_client.on_connect = self.on_connect
        self._sub_client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.debug("Mqtt native connection OK!")
            # subscribe the registered topics
            for subscription in self._topics_to_subscribe:
                client.subscribe(subscription.topic)
        else:
            logger.error("Bad connection request, return code: %d", rc)

    def on_disconnect(self, client, userdata, rc):
        logger.debug("Mqtt native is disconnected with reason:  %d", rc)

    async def init(self):
        if self._port is None:
            self._sub_client.connect(self._hostname)
            self._pub_client.connect(self._hostname)
        else:
            self._sub_client.connect(self._hostname, self._port)
            self._pub_client.connect(self._hostname, self._port)

    async def run(self):
        self._sub_client.loop_start()
        self._pub_client.loop_start()

    async def subscribe_topic(self, topic, coro):
        self._topics_to_subscribe.append(MqttTopicSubscription(topic, coro))
        if self._sub_client.is_connected():
            self._sub_client.subscribe(topic)

        loop = asyncio.get_event_loop()

        @self._sub_client.topic_callback(topic)
        def handle(client, userdata, msg):
            try:
                message = str(msg.payload.decode("utf-8"))
            except UnicodeDecodeError as err:
                logger.error(err)
                return
            if asyncio.iscoroutinefunction(coro):
                # run the async callbacks on the main event loop
                asyncio.run_coroutine_threadsafe(coro(message), loop)
            else:
                coro(message)

    async def publish_event(self, topic: str, data: str):
        return self._pub_client.publish(topic, data)
