/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.cdk.integrations.destination.async.model.AirbyteRecordMessageFile
import io.airbyte.cdk.integrations.destination.s3.FileUploadFormat
import io.airbyte.cdk.integrations.destination.s3.S3BaseDestinationAcceptanceTest
import io.airbyte.cdk.integrations.destination.s3.util.Flattening
import io.airbyte.commons.features.EnvVariableFeatureFlags
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.*
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.File
import java.time.Instant
import kotlin.io.path.createDirectories
import kotlin.io.path.createFile
import kotlin.io.path.writeText
import kotlin.random.Random
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.Test
import kotlin.io.path.absolute

private val LOGGER = KotlinLogging.logger {}

class S3FileTransferDestinationTest : S3BaseDestinationAcceptanceTest() {
    override val supportsFileTransfer = true
    override val formatConfig: JsonNode
        get() =
            Jsons.jsonNode(
                java.util.Map.of(
                    "format_type",
                    FileUploadFormat.CSV,
                    "flattening",
                    Flattening.ROOT_LEVEL.value,
                    "compression",
                    Jsons.jsonNode(java.util.Map.of("compression_type", "No Compression"))
                )
            )

    private fun getStreamCompleteMessage(streamName: String): AirbyteMessage {
        return AirbyteMessage()
            .withType(AirbyteMessage.Type.TRACE)
            .withTrace(
                AirbyteTraceMessage()
                    .withStreamStatus(
                        AirbyteStreamStatusTraceMessage()
                            .withStatus(
                                AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE
                            )
                            .withStreamDescriptor(StreamDescriptor().withName(streamName))
                    )
            )
    }

    private fun createFakeFile(): File {
        val depth = Random.nextInt(10)
        val dirPath =
            (0..depth).joinToString("/") { "dir" + RandomStringUtils.insecure().nextAlphanumeric(5) }
        val fileName = "fakeFile" + RandomStringUtils.insecure().nextAlphanumeric(5)
        val filePath = "$dirPath/$fileName"
        val fileSize = 1_024 * 1_024
        LOGGER.info{ "SGX creating director $dirPath inside $fileTransferMountSource" }

        fileTransferMountSource!!.resolve(dirPath).createDirectories()
        LOGGER.info{ "SGX creating file $filePath inside $fileTransferMountSource"}
        fileTransferMountSource!!
            .resolve(filePath)
            .createFile()
            .writeText(RandomStringUtils.insecure().nextAlphanumeric(fileSize))
        return File(filePath)
    }

    private fun configureCatalog(streamName: String): ConfiguredAirbyteCatalog {
        val streamSchema = JsonNodeFactory.instance.objectNode()
        streamSchema.set<JsonNode>("properties", JsonNodeFactory.instance.objectNode())
        return ConfiguredAirbyteCatalog()
            .withStreams(
                java.util.List.of(
                    ConfiguredAirbyteStream()
                        .withSyncMode(SyncMode.INCREMENTAL)
                        .withDestinationSyncMode(DestinationSyncMode.APPEND_DEDUP)
                        .withGenerationId(0)
                        .withMinimumGenerationId(0)
                        .withSyncId(0)
                        .withStream(
                            AirbyteStream().withName(streamName).withJsonSchema(streamSchema)
                        ),
                ),
            )
    }

    private fun createMessageForFile(streamName: String, file: File): AirbyteMessage {
        val filePath =
            file.toPath().absolute().relativize(EnvVariableFeatureFlags.DEFAULT_AIRBYTE_STAGING_DIRECTORY.absolute())
        return AirbyteMessage()
            .withType(AirbyteMessage.Type.RECORD)
            .withRecord(
                AirbyteRecordMessage()
                    .withStream(streamName)
                    .withEmittedAt(Instant.now().toEpochMilli())
                    .withData(ObjectMapper().readTree("{}"))
                    .withAdditionalProperty(
                        "file",
                        AirbyteRecordMessageFile(
                            fileUrl = file.absolutePath,
                            bytes = file.length(),
                            fileRelativePath = "$filePath",
                            modified = 123456L,
                            sourceFileUrl = "//sftp-testing-for-file-transfer/$filePath",
                        )
                    )
            )
    }

    @Test
    fun testFakeFileTransfer() {
        LOGGER.info {
            "${EnvVariableFeatureFlags.DEFAULT_AIRBYTE_STAGING_DIRECTORY} is mounted from $fileTransferMountSource"
        }
        val streamName = "str" + RandomStringUtils.insecure().nextAlphanumeric(5)
        val file = createFakeFile()
        val catalog = configureCatalog(streamName)
        val recordMessage = createMessageForFile(streamName, file)

        runSyncAndVerifyStateOutput(
            getConfig(),
            listOf(recordMessage, getStreamCompleteMessage(streamName)),
            catalog,
            false
        )
    }
}
