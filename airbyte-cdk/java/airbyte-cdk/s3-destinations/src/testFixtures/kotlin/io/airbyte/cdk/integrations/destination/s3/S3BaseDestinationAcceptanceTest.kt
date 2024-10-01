/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.integrations.destination.s3

import com.amazonaws.services.s3.AmazonS3
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.integrations.destination.NamingConventionTransformer
import io.airbyte.cdk.integrations.destination.s3.util.S3NameTransformer
import io.airbyte.cdk.integrations.standardtest.destination.BaseDestinationAcceptanceTest
import io.airbyte.cdk.integrations.standardtest.destination.DestinationAcceptanceTest
import io.airbyte.commons.io.IOs
import io.airbyte.commons.json.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Path
import java.util.HashSet
import org.apache.commons.lang3.RandomStringUtils
import org.mockito.Mockito

private val LOGGER = KotlinLogging.logger {}

abstract class S3BaseDestinationAcceptanceTest() :
    BaseDestinationAcceptanceTest(
        verifyIndividualStateAndCounts = true,
    ) {
    protected val secretFilePath: String = "secrets/config.json"
    override val imageName: String
        get() = "airbyte/destination-s3:dev"
    protected var configJson: JsonNode? = null

    override fun getConfig(): JsonNode = configJson!!
    protected open val baseConfigJson: JsonNode
        get() = Jsons.deserialize(IOs.readFile(Path.of(secretFilePath)))
    protected abstract val formatConfig: JsonNode?
        get
    protected var s3DestinationConfig: S3DestinationConfig = Mockito.mock()
    protected var s3Client: AmazonS3? = null
    protected var s3nameTransformer: NamingConventionTransformer = Mockito.mock()
    protected var s3StorageOperations: S3StorageOperations? = null

    fun storageProvider(): StorageProvider {
        return StorageProvider.AWS_S3
    }

    /**
     * This method does the following:
     * * Construct the S3 destination config.
     * * Construct the S3 client.
     */
    override fun setup(
        testEnv: DestinationAcceptanceTest.TestDestinationEnv,
        TEST_SCHEMAS: HashSet<String>
    ) {
        val baseConfigJson = baseConfigJson
        // Set a random s3 bucket path for each integration test
        val configJson = Jsons.clone(baseConfigJson)
        val testBucketPath =
            String.format(
                "test_%s",
                RandomStringUtils.insecure().nextAlphanumeric(5),
            )
        (configJson as ObjectNode)
            .put("s3_bucket_path", testBucketPath)
            .set<JsonNode>("format", formatConfig)
        this.configJson = configJson
        this.s3DestinationConfig =
            S3DestinationConfig.getS3DestinationConfig(
                configJson,
                storageProvider(),
                getConnectorEnv()
            )
        LOGGER.info {
            "${"Test full path: {}/{}"} ${s3DestinationConfig.bucketName} ${s3DestinationConfig.bucketPath}"
        }

        this.s3Client = s3DestinationConfig.getS3Client()
        this.s3nameTransformer = S3NameTransformer()
        this.s3StorageOperations =
            S3StorageOperations(s3nameTransformer, s3Client!!, s3DestinationConfig)
    }
}
