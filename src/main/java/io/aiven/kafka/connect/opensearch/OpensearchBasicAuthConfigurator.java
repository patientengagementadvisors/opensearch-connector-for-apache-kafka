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

import java.util.Objects;

import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.opensearch.spi.OpensearchClientConfigurator;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

/**
 * Adds basic authentication to the {@index HttpAsyncClientBuilder} for Opensearch client if configured. Note:
 * Configuration definitions are contributed by OpensearchBasicAuthConfigContributor to avoid classloader issues with
 * ServiceLoader in Kafka Connect's isolated plugin classloader.
 */
public class OpensearchBasicAuthConfigurator implements OpensearchClientConfigurator {

    /**
     * Public no-argument constructor required by ServiceLoader.
     */
    public OpensearchBasicAuthConfigurator() {
        // Default constructor for ServiceLoader
    }

    @Override
    public boolean apply(final OpensearchSinkConnectorConfig config, final HttpAsyncClientBuilder builder) {
        if (!isAuthenticatedConnection(config)) {
            return false;
        }

        final var credentialsProvider = new BasicCredentialsProvider();
        for (final var httpHost : config.httpHosts()) {
            credentialsProvider.setCredentials(new AuthScope(httpHost),
                    new UsernamePasswordCredentials(connectionUsername(config), connectionPassword(config).value()));
        }

        builder.setDefaultCredentialsProvider(credentialsProvider);
        return true;
    }

    private static boolean isAuthenticatedConnection(final OpensearchSinkConnectorConfig config) {
        return Objects.nonNull(connectionUsername(config)) && Objects.nonNull(connectionPassword(config));
    }

    private static String connectionUsername(final OpensearchSinkConnectorConfig config) {
        return config.getString(OpensearchBasicAuthConfigContributor.CONNECTION_USERNAME_CONFIG);
    }

    private static Password connectionPassword(final OpensearchSinkConnectorConfig config) {
        return config.getPassword(OpensearchBasicAuthConfigContributor.CONNECTION_PASSWORD_CONFIG);
    }

}
