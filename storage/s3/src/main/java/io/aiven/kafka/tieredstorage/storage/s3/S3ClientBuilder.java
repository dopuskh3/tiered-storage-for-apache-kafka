/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.tieredstorage.storage.s3;

import java.util.Objects;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkAsyncHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;
import software.amazon.awssdk.utils.AttributeMap;

class S3ClientBuilder {

    public static enum ClientType {
        DOWNLOAD, UPLOAD
    }

    static S3AsyncClient build(final S3StorageConfig config) {
        return build(config, ClientType.DOWNLOAD);
    }

    static S3AsyncClient build(final S3StorageConfig config, final ClientType clientType) {
        if (config.crtEnabled()) {
            return buildCtrAsyncClient(config, clientType);
        }
        return buildAsyncClient(config);
    }

    static S3AsyncClient buildCtrAsyncClient(final S3StorageConfig config, final ClientType clientType) {
        final S3CrtAsyncClientBuilder s3ClientBuilder = S3AsyncClient.crtBuilder();
        final Region region = config.region();
        if (Objects.isNull(config.s3ServiceEndpoint())) {
            s3ClientBuilder.region(region);
        } else {
            s3ClientBuilder.region(region)
                .endpointOverride(config.s3ServiceEndpoint());
        }
        if (config.pathStyleAccessEnabled() != null) {
            s3ClientBuilder.forcePathStyle(config.pathStyleAccessEnabled());
        }

        if (config.crtUploadThroughputGb() > 0) {
            if (clientType.equals(ClientType.UPLOAD)) {
                s3ClientBuilder.targetThroughputInGbps(config.crtUploadThroughputGb());
            }
        }

        final var httpConfig =
            S3CrtHttpConfiguration.builder().trustAllCertificatesEnabled(!config.certificateCheckEnabled())
                .connectionTimeout(config.apiCallTimeout()).build();
        s3ClientBuilder.httpConfiguration(httpConfig);

        s3ClientBuilder.checksumValidationEnabled(config.checksumCheckEnabled());
        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        if (credentialsProvider != null) {
            s3ClientBuilder.credentialsProvider(credentialsProvider);
        }

        return s3ClientBuilder.build();
    }

    static S3AsyncClient buildAsyncClient(final S3StorageConfig config) {
        final software.amazon.awssdk.services.s3.S3AsyncClientBuilder s3ClientBuilder = S3AsyncClient.builder();
        final Region region = config.region();
        if (Objects.isNull(config.s3ServiceEndpoint())) {
            s3ClientBuilder.region(region);
        } else {
            s3ClientBuilder.region(region)
                .endpointOverride(config.s3ServiceEndpoint());
        }
        if (config.pathStyleAccessEnabled() != null) {
            s3ClientBuilder.forcePathStyle(config.pathStyleAccessEnabled());
        }
        s3ClientBuilder.httpClient(NettyNioAsyncHttpClient.builder().build());
        if (!config.certificateCheckEnabled()) {
            s3ClientBuilder.httpClient(
                new DefaultSdkAsyncHttpClientBuilder()
                    .buildWithDefaults(
                        AttributeMap.builder()
                            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                            .build()
                    )
            );
        }

        s3ClientBuilder.serviceConfiguration(builder ->
            builder.checksumValidationEnabled(config.checksumCheckEnabled()));

        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        if (credentialsProvider != null) {
            s3ClientBuilder.credentialsProvider(credentialsProvider);
        }
        s3ClientBuilder.overrideConfiguration(c -> {
            c.addMetricPublisher(new MetricCollector());
            c.apiCallTimeout(config.apiCallTimeout());
            c.apiCallAttemptTimeout(config.apiCallAttemptTimeout());
        });
        return s3ClientBuilder.build();
    }
}
