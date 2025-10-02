/*
 * Copyright 2019 Aiven Oy
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
package io.aiven.kafka.connect.opensearch;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.aiven.kafka.connect.opensearch.spi.ConfigDefContributor;

/**
 * Contributes basic authentication configuration definitions. This class is separate from
 * OpensearchBasicAuthConfigurator to avoid classloader issues with ServiceLoader in Kafka Connect's isolated plugin
 * classloader.
 */
public class OpensearchBasicAuthConfigContributor implements ConfigDefContributor {
    public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
    private static final String CONNECTION_USERNAME_DOC = "The username used to authenticate with OpenSearch. "
            + "The default is the null, and authentication will only be performed if "
            + " both the username and password are non-null.";
    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "The password used to authenticate with OpenSearch. "
            + "The default is the null, and authentication will only be performed if "
            + " both the username and password are non-null.";

    /**
     * Public no-argument constructor required by ServiceLoader.
     */
    public OpensearchBasicAuthConfigContributor() {
        // Default constructor for ServiceLoader
    }

    @Override
    public void addConfig(final ConfigDef config) {
        config.define(CONNECTION_USERNAME_CONFIG, Type.STRING, null, Importance.MEDIUM, CONNECTION_USERNAME_DOC,
                "Authentication", 0, Width.SHORT, "Connection Username")
                .define(CONNECTION_PASSWORD_CONFIG, Type.PASSWORD, null, Importance.MEDIUM, CONNECTION_PASSWORD_DOC,
                        "Authentication", 1, Width.SHORT, "Connection Password");
    }
}
