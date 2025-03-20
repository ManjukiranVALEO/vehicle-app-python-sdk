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

import sys
import logging
from urllib.parse import urlparse

from velocitas_sdk.base import Middleware, MiddlewareType
from velocitas_sdk.native.locator import NativeServiceLocator
from velocitas_sdk.native.mqtt import MqttClient

logger = logging.getLogger(__name__)

class NativeMiddleware(Middleware):
    """Native middleware implementation."""

    def __init__(self) -> None:
        super().__init__()

        self.type = MiddlewareType.NATIVE
        self.service_locator = NativeServiceLocator()

        _address = self.service_locator.get_service_location("mqtt")
        _port = urlparse(_address).port
        _hostname = urlparse(_address).hostname
        _cacert, _key, _device_cert = self.service_locator.get_certificates("mqtt")
        _proxy_address = self.service_locator.get_service_location("mqtt_proxy")
        if _hostname is None:
            print("No hostname")
            sys.exit(-1)
        if _cacert is None:
            self.pubsub_client = MqttClient(hostname=_hostname, port=_port)
        else:
            logger.info("Using certificates to connect to mqtt broker")
            if _proxy_address is None:
                self.pubsub_client = MqttClient(hostname=_hostname, port=_port, cacert=_cacert, key=_key, device_cert=_device_cert)
            else:
                logger.info("Using proxy to connect to mqtt broker")
                _proxy_port = urlparse(_proxy_address).port
                _proxy_hostname = urlparse(_proxy_address).hostname
                self.pubsub_client = MqttClient(hostname=_hostname, port=_port, cacert=_cacert, key=_key, device_cert=_device_cert, proxy_hostname=_proxy_hostname, proxy_port=_proxy_port)

    async def start(self):
        await self.pubsub_client.init()

    async def wait_until_ready(self):
        pass

    async def stop(self):
        pass
