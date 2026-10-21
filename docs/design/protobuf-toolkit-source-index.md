# Toolkit source index

Baseline: `61955b4f974466783f1e7af42e31ba97016a417d`. Generated from tracked-location file inventory on 2026-10-13.

This is a navigation inventory, not a claim that each file was individually reviewed or each test executed. See [audit findings](protobuf-toolkit-audit.md) for inspected behavior and conclusions. Nested Gradle modules are listed separately.

## acquire/gather/core

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/acquire/gather/CompositeProtoGatherer.java](../../acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/CompositeProtoGatherer.java)
- [java/ai/protomolt/proto/acquire/gather/FilesystemProtoGatherer.java](../../acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/FilesystemProtoGatherer.java)
- [java/ai/protomolt/proto/acquire/gather/GatherException.java](../../acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/GatherException.java)
- [java/ai/protomolt/proto/acquire/gather/GatheringDescriptorLoader.java](../../acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/GatheringDescriptorLoader.java)
- [java/ai/protomolt/proto/acquire/gather/JarProtoExtraction.java](../../acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/JarProtoExtraction.java)
- [java/ai/protomolt/proto/acquire/gather/JarProtoGatherer.java](../../acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/JarProtoGatherer.java)
- [java/ai/protomolt/proto/acquire/gather/ProtoGatherer.java](../../acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/ProtoGatherer.java)
- [java/ai/protomolt/proto/acquire/gather/package-info.java](../../acquire/gather/core/src/main/java/ai/protomolt/proto/acquire/gather/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/acquire/gather/CompositeProtoGathererTest.java](../../acquire/gather/core/src/test/java/ai/protomolt/proto/acquire/gather/CompositeProtoGathererTest.java)
- [java/ai/protomolt/proto/acquire/gather/FilesystemProtoGathererTest.java](../../acquire/gather/core/src/test/java/ai/protomolt/proto/acquire/gather/FilesystemProtoGathererTest.java)
- [java/ai/protomolt/proto/acquire/gather/GatherExceptionTest.java](../../acquire/gather/core/src/test/java/ai/protomolt/proto/acquire/gather/GatherExceptionTest.java)
- [java/ai/protomolt/proto/acquire/gather/GatheringDescriptorLoaderTest.java](../../acquire/gather/core/src/test/java/ai/protomolt/proto/acquire/gather/GatheringDescriptorLoaderTest.java)
- [java/ai/protomolt/proto/acquire/gather/JarProtoExtractionTest.java](../../acquire/gather/core/src/test/java/ai/protomolt/proto/acquire/gather/JarProtoExtractionTest.java)
- [java/ai/protomolt/proto/acquire/gather/JarProtoGathererTest.java](../../acquire/gather/core/src/test/java/ai/protomolt/proto/acquire/gather/JarProtoGathererTest.java)

## acquire/gather/git

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/acquire/gather/git/GatherGitAction.java](../../acquire/gather/git/src/main/java/ai/protomolt/proto/acquire/gather/git/GatherGitAction.java)
- [java/ai/protomolt/proto/acquire/gather/git/GitCloneCache.java](../../acquire/gather/git/src/main/java/ai/protomolt/proto/acquire/gather/git/GitCloneCache.java)
- [java/ai/protomolt/proto/acquire/gather/git/GitProtoGatherer.java](../../acquire/gather/git/src/main/java/ai/protomolt/proto/acquire/gather/git/GitProtoGatherer.java)
- [java/ai/protomolt/proto/acquire/gather/git/package-info.java](../../acquire/gather/git/src/main/java/ai/protomolt/proto/acquire/gather/git/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/acquire/gather/git/GatherGitActionTest.java](../../acquire/gather/git/src/test/java/ai/protomolt/proto/acquire/gather/git/GatherGitActionTest.java)
- [java/ai/protomolt/proto/acquire/gather/git/GitProtoGathererTest.java](../../acquire/gather/git/src/test/java/ai/protomolt/proto/acquire/gather/git/GitProtoGathererTest.java)

## acquire/gather/maven

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/acquire/gather/maven/MavenProtoGatherer.java](../../acquire/gather/maven/src/main/java/ai/protomolt/proto/acquire/gather/maven/MavenProtoGatherer.java)
- [java/ai/protomolt/proto/acquire/gather/maven/package-info.java](../../acquire/gather/maven/src/main/java/ai/protomolt/proto/acquire/gather/maven/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/acquire/gather/maven/MavenProtoGathererTest.java](../../acquire/gather/maven/src/test/java/ai/protomolt/proto/acquire/gather/maven/MavenProtoGathererTest.java)

## core/acp

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/acp/AcpAgent.java](../../core/acp/src/main/java/ai/protomolt/proto/acp/AcpAgent.java)
- [java/ai/protomolt/proto/acp/AcpClient.java](../../core/acp/src/main/java/ai/protomolt/proto/acp/AcpClient.java)
- [java/ai/protomolt/proto/acp/AcpConnection.java](../../core/acp/src/main/java/ai/protomolt/proto/acp/AcpConnection.java)
- [java/ai/protomolt/proto/acp/AcpError.java](../../core/acp/src/main/java/ai/protomolt/proto/acp/AcpError.java)
- [java/ai/protomolt/proto/acp/PromptContext.java](../../core/acp/src/main/java/ai/protomolt/proto/acp/PromptContext.java)
- [java/ai/protomolt/proto/acp/PromptHandler.java](../../core/acp/src/main/java/ai/protomolt/proto/acp/PromptHandler.java)
- [java/ai/protomolt/proto/acp/package-info.java](../../core/acp/src/main/java/ai/protomolt/proto/acp/package-info.java)
- [java/ai/protomolt/proto/msp/MspClient.java](../../core/acp/src/main/java/ai/protomolt/proto/msp/MspClient.java)
- [java/ai/protomolt/proto/msp/MspError.java](../../core/acp/src/main/java/ai/protomolt/proto/msp/MspError.java)
- [java/ai/protomolt/proto/msp/Uuid7.java](../../core/acp/src/main/java/ai/protomolt/proto/msp/Uuid7.java)
- [java/ai/protomolt/proto/msp/package-info.java](../../core/acp/src/main/java/ai/protomolt/proto/msp/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/acp/AcpAgentTest.java](../../core/acp/src/test/java/ai/protomolt/proto/acp/AcpAgentTest.java)
- [java/ai/protomolt/proto/acp/AcpClientTest.java](../../core/acp/src/test/java/ai/protomolt/proto/acp/AcpClientTest.java)
- [java/ai/protomolt/proto/acp/AcpConnectionTest.java](../../core/acp/src/test/java/ai/protomolt/proto/acp/AcpConnectionTest.java)
- [java/ai/protomolt/proto/acp/AcpWireTranscriptTest.java](../../core/acp/src/test/java/ai/protomolt/proto/acp/AcpWireTranscriptTest.java)
- [java/ai/protomolt/proto/msp/MspClientTest.java](../../core/acp/src/test/java/ai/protomolt/proto/msp/MspClientTest.java)

## core/cel

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/cel/CelCompilationException.java](../../core/cel/src/main/java/ai/protomolt/proto/cel/CelCompilationException.java)
- [java/ai/protomolt/proto/cel/CelEnvironmentFactory.java](../../core/cel/src/main/java/ai/protomolt/proto/cel/CelEnvironmentFactory.java)
- [java/ai/protomolt/proto/cel/CelEvaluationException.java](../../core/cel/src/main/java/ai/protomolt/proto/cel/CelEvaluationException.java)
- [java/ai/protomolt/proto/cel/CelEvaluator.java](../../core/cel/src/main/java/ai/protomolt/proto/cel/CelEvaluator.java)
- [java/ai/protomolt/proto/cel/CelValidation.java](../../core/cel/src/main/java/ai/protomolt/proto/cel/CelValidation.java)
- [java/ai/protomolt/proto/cel/package-info.java](../../core/cel/src/main/java/ai/protomolt/proto/cel/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/cel/CelEnvironmentFactoryTest.java](../../core/cel/src/test/java/ai/protomolt/proto/cel/CelEnvironmentFactoryTest.java)
- [java/ai/protomolt/proto/cel/CelEvaluatorBehaviorTest.java](../../core/cel/src/test/java/ai/protomolt/proto/cel/CelEvaluatorBehaviorTest.java)
- [java/ai/protomolt/proto/cel/CelEvaluatorTest.java](../../core/cel/src/test/java/ai/protomolt/proto/cel/CelEvaluatorTest.java)
- [java/ai/protomolt/proto/cel/CelFixtures.java](../../core/cel/src/test/java/ai/protomolt/proto/cel/CelFixtures.java)
- [java/ai/protomolt/proto/cel/CelValidationContractTest.java](../../core/cel/src/test/java/ai/protomolt/proto/cel/CelValidationContractTest.java)
- [java/ai/protomolt/proto/cel/CelValidationTest.java](../../core/cel/src/test/java/ai/protomolt/proto/cel/CelValidationTest.java)

## core/codegen

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/codegen/GenerateStubsAction.java](../../core/codegen/src/main/java/ai/protomolt/proto/codegen/GenerateStubsAction.java)
- [java/ai/protomolt/proto/codegen/WasmProtoc.java](../../core/codegen/src/main/java/ai/protomolt/proto/codegen/WasmProtoc.java)
- [java/ai/protomolt/proto/codegen/package-info.java](../../core/codegen/src/main/java/ai/protomolt/proto/codegen/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/codegen/GenerateStubsActionMetadataTest.java](../../core/codegen/src/test/java/ai/protomolt/proto/codegen/GenerateStubsActionMetadataTest.java)
- [java/ai/protomolt/proto/codegen/GenerateStubsActionTest.java](../../core/codegen/src/test/java/ai/protomolt/proto/codegen/GenerateStubsActionTest.java)
- [java/ai/protomolt/proto/codegen/GenerateStubsActionValidationTest.java](../../core/codegen/src/test/java/ai/protomolt/proto/codegen/GenerateStubsActionValidationTest.java)
- [java/ai/protomolt/proto/codegen/WasmProtocTest.java](../../core/codegen/src/test/java/ai/protomolt/proto/codegen/WasmProtocTest.java)

## core/compat

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/compat/ChangeRules.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/ChangeRules.java)
- [java/ai/protomolt/proto/compat/CompatibilityChecker.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/CompatibilityChecker.java)
- [java/ai/protomolt/proto/compat/CompatibilityException.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/CompatibilityException.java)
- [java/ai/protomolt/proto/compat/CompatibilityMode.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/CompatibilityMode.java)
- [java/ai/protomolt/proto/compat/CompatibilityResult.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/CompatibilityResult.java)
- [java/ai/protomolt/proto/compat/DescriptorIndex.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/DescriptorIndex.java)
- [java/ai/protomolt/proto/compat/DescriptorSnippets.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/DescriptorSnippets.java)
- [java/ai/protomolt/proto/compat/DiffImpacts.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/DiffImpacts.java)
- [java/ai/protomolt/proto/compat/EnumDiff.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/EnumDiff.java)
- [java/ai/protomolt/proto/compat/FieldTypeDiff.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/FieldTypeDiff.java)
- [java/ai/protomolt/proto/compat/Impact.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/Impact.java)
- [java/ai/protomolt/proto/compat/IncompatibleSchemaException.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/IncompatibleSchemaException.java)
- [java/ai/protomolt/proto/compat/MessageDiff.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/MessageDiff.java)
- [java/ai/protomolt/proto/compat/MessageInfo.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/MessageInfo.java)
- [java/ai/protomolt/proto/compat/OneofDiff.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/OneofDiff.java)
- [java/ai/protomolt/proto/compat/ReservedRanges.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/ReservedRanges.java)
- [java/ai/protomolt/proto/compat/SchemaChange.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/SchemaChange.java)
- [java/ai/protomolt/proto/compat/SchemaDiff.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/SchemaDiff.java)
- [java/ai/protomolt/proto/compat/ServiceDiff.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/ServiceDiff.java)
- [java/ai/protomolt/proto/compat/package-info.java](../../core/compat/src/main/java/ai/protomolt/proto/compat/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/compat/CompatibilityCheckerTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/CompatibilityCheckerTest.java)
- [java/ai/protomolt/proto/compat/CompatibilityModeTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/CompatibilityModeTest.java)
- [java/ai/protomolt/proto/compat/CompatibilityResultTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/CompatibilityResultTest.java)
- [java/ai/protomolt/proto/compat/EnumRuleTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/EnumRuleTest.java)
- [java/ai/protomolt/proto/compat/FieldRuleTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/FieldRuleTest.java)
- [java/ai/protomolt/proto/compat/HistoryCheckTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/HistoryCheckTest.java)
- [java/ai/protomolt/proto/compat/OneofMapRuleTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/OneofMapRuleTest.java)
- [java/ai/protomolt/proto/compat/Proto2RuleTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/Proto2RuleTest.java)
- [java/ai/protomolt/proto/compat/ReservedRuleTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/ReservedRuleTest.java)
- [java/ai/protomolt/proto/compat/ServiceRuleTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/ServiceRuleTest.java)
- [java/ai/protomolt/proto/compat/TestSchemas.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/TestSchemas.java)
- [java/ai/protomolt/proto/compat/TypeChangeRuleTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/TypeChangeRuleTest.java)
- [java/ai/protomolt/proto/compat/TypeLevelRuleTest.java](../../core/compat/src/test/java/ai/protomolt/proto/compat/TypeLevelRuleTest.java)

## core/descriptors

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/descriptors/ClasspathDescriptorLoader.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/ClasspathDescriptorLoader.java)
- [java/ai/protomolt/proto/descriptors/ClosedDescriptorSet.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/ClosedDescriptorSet.java)
- [java/ai/protomolt/proto/descriptors/DescriptorCatalog.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/DescriptorCatalog.java)
- [java/ai/protomolt/proto/descriptors/DescriptorFingerprints.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/DescriptorFingerprints.java)
- [java/ai/protomolt/proto/descriptors/DescriptorLoader.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/DescriptorLoader.java)
- [java/ai/protomolt/proto/descriptors/DescriptorRegistry.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/DescriptorRegistry.java)
- [java/ai/protomolt/proto/descriptors/GoogleDescriptorLoader.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/GoogleDescriptorLoader.java)
- [java/ai/protomolt/proto/descriptors/MessageFingerprints.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/MessageFingerprints.java)
- [java/ai/protomolt/proto/descriptors/MessageWireBudget.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/MessageWireBudget.java)
- [java/ai/protomolt/proto/descriptors/package-info.java](../../core/descriptors/src/main/java/ai/protomolt/proto/descriptors/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/descriptors/ClasspathDescriptorLoaderEdgeCasesTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/ClasspathDescriptorLoaderEdgeCasesTest.java)
- [java/ai/protomolt/proto/descriptors/ClasspathDescriptorLoaderTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/ClasspathDescriptorLoaderTest.java)
- [java/ai/protomolt/proto/descriptors/ClosedDescriptorSetTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/ClosedDescriptorSetTest.java)
- [java/ai/protomolt/proto/descriptors/DescriptorCatalogTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/DescriptorCatalogTest.java)
- [java/ai/protomolt/proto/descriptors/DescriptorFingerprintsTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/DescriptorFingerprintsTest.java)
- [java/ai/protomolt/proto/descriptors/DescriptorLoaderContractTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/DescriptorLoaderContractTest.java)
- [java/ai/protomolt/proto/descriptors/DescriptorRegistryBehaviorTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/DescriptorRegistryBehaviorTest.java)
- [java/ai/protomolt/proto/descriptors/DescriptorRegistryLoaderTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/DescriptorRegistryLoaderTest.java)
- [java/ai/protomolt/proto/descriptors/DescriptorRegistryTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/DescriptorRegistryTest.java)
- [java/ai/protomolt/proto/descriptors/GoogleDescriptorLoaderFromSetTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/GoogleDescriptorLoaderFromSetTest.java)
- [java/ai/protomolt/proto/descriptors/GoogleDescriptorLoaderTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/GoogleDescriptorLoaderTest.java)
- [java/ai/protomolt/proto/descriptors/MessageFingerprintsTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/MessageFingerprintsTest.java)
- [java/ai/protomolt/proto/descriptors/MessageWireBudgetTest.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/MessageWireBudgetTest.java)
- [java/ai/protomolt/proto/descriptors/fixture/Deep.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/fixture/Deep.java)
- [java/ai/protomolt/proto/descriptors/fixture/GeneratedLike.java](../../core/descriptors/src/test/java/ai/protomolt/proto/descriptors/fixture/GeneratedLike.java)

## core/formats

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/formats/Decimals.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Decimals.java)
- [java/ai/protomolt/proto/formats/Emails.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Emails.java)
- [java/ai/protomolt/proto/formats/Encodings.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Encodings.java)
- [java/ai/protomolt/proto/formats/Endpoints.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Endpoints.java)
- [java/ai/protomolt/proto/formats/Formats.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Formats.java)
- [java/ai/protomolt/proto/formats/HostAndPort.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/HostAndPort.java)
- [java/ai/protomolt/proto/formats/Hostnames.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Hostnames.java)
- [java/ai/protomolt/proto/formats/Identifiers.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Identifiers.java)
- [java/ai/protomolt/proto/formats/IpAddresses.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/IpAddresses.java)
- [java/ai/protomolt/proto/formats/MediaTypes.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/MediaTypes.java)
- [java/ai/protomolt/proto/formats/PhoneNumbers.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/PhoneNumbers.java)
- [java/ai/protomolt/proto/formats/PostalMasks.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/PostalMasks.java)
- [java/ai/protomolt/proto/formats/Rfc3986.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Rfc3986.java)
- [java/ai/protomolt/proto/formats/Temporals.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Temporals.java)
- [java/ai/protomolt/proto/formats/TradeItemNumbers.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/TradeItemNumbers.java)
- [java/ai/protomolt/proto/formats/Vocabularies.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/Vocabularies.java)
- [java/ai/protomolt/proto/formats/package-info.java](../../core/formats/src/main/java/ai/protomolt/proto/formats/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/formats/DecimalsTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/DecimalsTest.java)
- [java/ai/protomolt/proto/formats/EmailsTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/EmailsTest.java)
- [java/ai/protomolt/proto/formats/EndpointsTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/EndpointsTest.java)
- [java/ai/protomolt/proto/formats/FormatsTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/FormatsTest.java)
- [java/ai/protomolt/proto/formats/HostAndPortTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/HostAndPortTest.java)
- [java/ai/protomolt/proto/formats/HostnamesTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/HostnamesTest.java)
- [java/ai/protomolt/proto/formats/IdentifiersTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/IdentifiersTest.java)
- [java/ai/protomolt/proto/formats/IpAddressesParseTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/IpAddressesParseTest.java)
- [java/ai/protomolt/proto/formats/IpAddressesTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/IpAddressesTest.java)
- [java/ai/protomolt/proto/formats/PostalMasksTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/PostalMasksTest.java)
- [java/ai/protomolt/proto/formats/Rfc3986Test.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/Rfc3986Test.java)
- [java/ai/protomolt/proto/formats/TradeItemNumbersTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/TradeItemNumbersTest.java)
- [java/ai/protomolt/proto/formats/WellKnownFormatsTest.java](../../core/formats/src/test/java/ai/protomolt/proto/formats/WellKnownFormatsTest.java)

## core/helpers

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/helpers/AnyHandler.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/AnyHandler.java)
- [java/ai/protomolt/proto/helpers/BinaryProtobufIdentifierValidator.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/BinaryProtobufIdentifierValidator.java)
- [java/ai/protomolt/proto/helpers/MappingHelper.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/MappingHelper.java)
- [java/ai/protomolt/proto/helpers/MappingHelperJsonSupport.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/MappingHelperJsonSupport.java)
- [java/ai/protomolt/proto/helpers/MessageDiff.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/MessageDiff.java)
- [java/ai/protomolt/proto/helpers/PayloadCodec.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/PayloadCodec.java)
- [java/ai/protomolt/proto/helpers/ProtoFqnConflictDetector.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/ProtoFqnConflictDetector.java)
- [java/ai/protomolt/proto/helpers/ProtoSchemaValidationException.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/ProtoSchemaValidationException.java)
- [java/ai/protomolt/proto/helpers/TypeConverter.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/TypeConverter.java)
- [java/ai/protomolt/proto/helpers/package-info.java](../../core/helpers/src/main/java/ai/protomolt/proto/helpers/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/helpers/AnyHandlerTest.java](../../core/helpers/src/test/java/ai/protomolt/proto/helpers/AnyHandlerTest.java)
- [java/ai/protomolt/proto/helpers/BinaryProtobufIdentifierValidatorTest.java](../../core/helpers/src/test/java/ai/protomolt/proto/helpers/BinaryProtobufIdentifierValidatorTest.java)
- [java/ai/protomolt/proto/helpers/MappingHelperJsonSupportTest.java](../../core/helpers/src/test/java/ai/protomolt/proto/helpers/MappingHelperJsonSupportTest.java)
- [java/ai/protomolt/proto/helpers/MappingHelperTest.java](../../core/helpers/src/test/java/ai/protomolt/proto/helpers/MappingHelperTest.java)
- [java/ai/protomolt/proto/helpers/MessageDiffTest.java](../../core/helpers/src/test/java/ai/protomolt/proto/helpers/MessageDiffTest.java)
- [java/ai/protomolt/proto/helpers/PayloadCodecTest.java](../../core/helpers/src/test/java/ai/protomolt/proto/helpers/PayloadCodecTest.java)
- [java/ai/protomolt/proto/helpers/ProtoFqnConflictDetectorTest.java](../../core/helpers/src/test/java/ai/protomolt/proto/helpers/ProtoFqnConflictDetectorTest.java)
- [java/ai/protomolt/proto/helpers/TypeConverterTest.java](../../core/helpers/src/test/java/ai/protomolt/proto/helpers/TypeConverterTest.java)

## core/sources

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/sources/CompiledProtos.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/CompiledProtos.java)
- [java/ai/protomolt/proto/sources/LinkedOptionsEncoder.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/LinkedOptionsEncoder.java)
- [java/ai/protomolt/proto/sources/LinkedOptionsRepair.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/LinkedOptionsRepair.java)
- [java/ai/protomolt/proto/sources/OptionEncodingException.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/OptionEncodingException.java)
- [java/ai/protomolt/proto/sources/OptionStrippingRewriter.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/OptionStrippingRewriter.java)
- [java/ai/protomolt/proto/sources/ProtoCompilationException.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/ProtoCompilationException.java)
- [java/ai/protomolt/proto/sources/ProtoImports.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/ProtoImports.java)
- [java/ai/protomolt/proto/sources/ProtoSource.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/ProtoSource.java)
- [java/ai/protomolt/proto/sources/ProtoSourceCompiler.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/ProtoSourceCompiler.java)
- [java/ai/protomolt/proto/sources/ProtoSourceSet.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/ProtoSourceSet.java)
- [java/ai/protomolt/proto/sources/package-info.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/package-info.java)
- [java/ai/protomolt/proto/sources/publish/PublishOptions.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/publish/PublishOptions.java)
- [java/ai/protomolt/proto/sources/publish/PublishResult.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/publish/PublishResult.java)
- [java/ai/protomolt/proto/sources/publish/SchemaPublishException.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/publish/SchemaPublishException.java)
- [java/ai/protomolt/proto/sources/publish/SchemaPublisher.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/publish/SchemaPublisher.java)
- [java/ai/protomolt/proto/sources/publish/SubjectNamingStrategy.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/publish/SubjectNamingStrategy.java)
- [java/ai/protomolt/proto/sources/publish/package-info.java](../../core/sources/src/main/java/ai/protomolt/proto/sources/publish/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/sources/CompiledProtosTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/CompiledProtosTest.java)
- [java/ai/protomolt/proto/sources/OptionStrippingRewriterTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/OptionStrippingRewriterTest.java)
- [java/ai/protomolt/proto/sources/ProtoImportsTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoImportsTest.java)
- [java/ai/protomolt/proto/sources/ProtoSourceCompilerMapTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoSourceCompilerMapTest.java)
- [java/ai/protomolt/proto/sources/ProtoSourceCompilerOptionsRepairTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoSourceCompilerOptionsRepairTest.java)
- [java/ai/protomolt/proto/sources/ProtoSourceCompilerOptionsTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoSourceCompilerOptionsTest.java)
- [java/ai/protomolt/proto/sources/ProtoSourceCompilerStructureTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoSourceCompilerStructureTest.java)
- [java/ai/protomolt/proto/sources/ProtoSourceCompilerTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoSourceCompilerTest.java)
- [java/ai/protomolt/proto/sources/ProtoSourceSetGraphTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoSourceSetGraphTest.java)
- [java/ai/protomolt/proto/sources/ProtoSourceSetTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoSourceSetTest.java)
- [java/ai/protomolt/proto/sources/ProtoSourceTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/ProtoSourceTest.java)
- [java/ai/protomolt/proto/sources/publish/PublishModelTest.java](../../core/sources/src/test/java/ai/protomolt/proto/sources/publish/PublishModelTest.java)
- [resources/ai/protomolt/proto/llm/v1/llm.proto](../../core/sources/src/test/resources/ai/protomolt/proto/llm/v1/llm.proto)
- [resources/ai/protomolt/proto/meta/v1/metadata.proto](../../core/sources/src/test/resources/ai/protomolt/proto/meta/v1/metadata.proto)
- [resources/ai/protomolt/proto/prompt/testdata/v1/form.proto](../../core/sources/src/test/resources/ai/protomolt/proto/prompt/testdata/v1/form.proto)
- [resources/ai/protomolt/proto/quality/v1/quality.proto](../../core/sources/src/test/resources/ai/protomolt/proto/quality/v1/quality.proto)
- [resources/ai/protomolt/proto/validate/v1/validate.proto](../../core/sources/src/test/resources/ai/protomolt/proto/validate/v1/validate.proto)

## protobuf/llm

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/llm/DescriptorLlm.java](../../protobuf/llm/src/main/java/ai/protomolt/proto/llm/DescriptorLlm.java)
- [java/ai/protomolt/proto/llm/package-info.java](../../protobuf/llm/src/main/java/ai/protomolt/proto/llm/package-info.java)
- [proto/ai/protomolt/proto/llm/v1/llm.proto](../../protobuf/llm/src/main/proto/ai/protomolt/proto/llm/v1/llm.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/llm/DescriptorLlmRegistryTest.java](../../protobuf/llm/src/test/java/ai/protomolt/proto/llm/DescriptorLlmRegistryTest.java)
- [java/ai/protomolt/proto/llm/DescriptorLlmTest.java](../../protobuf/llm/src/test/java/ai/protomolt/proto/llm/DescriptorLlmTest.java)
- [proto/ai/protomolt/proto/llm/testdata/v1/annotated_form.proto](../../protobuf/llm/src/test/proto/ai/protomolt/proto/llm/testdata/v1/annotated_form.proto)

## protobuf/metadata

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/meta/DescriptorMetadata.java](../../protobuf/metadata/src/main/java/ai/protomolt/proto/meta/DescriptorMetadata.java)
- [java/ai/protomolt/proto/meta/SensitivityMasker.java](../../protobuf/metadata/src/main/java/ai/protomolt/proto/meta/SensitivityMasker.java)
- [java/ai/protomolt/proto/meta/package-info.java](../../protobuf/metadata/src/main/java/ai/protomolt/proto/meta/package-info.java)
- [proto/ai/protomolt/proto/meta/v1/metadata.proto](../../protobuf/metadata/src/main/proto/ai/protomolt/proto/meta/v1/metadata.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/meta/DescriptorMetadataTest.java](../../protobuf/metadata/src/test/java/ai/protomolt/proto/meta/DescriptorMetadataTest.java)
- [java/ai/protomolt/proto/meta/SensitivityMaskerTest.java](../../protobuf/metadata/src/test/java/ai/protomolt/proto/meta/SensitivityMaskerTest.java)
- [proto/ai/protomolt/proto/meta/testdata/v1/annotated_doc.proto](../../protobuf/metadata/src/test/proto/ai/protomolt/proto/meta/testdata/v1/annotated_doc.proto)

## protobuf/metric

### Production sources, contracts and SPI registrations

- [proto/ai/protomolt/proto/metric/v1/metric.proto](../../protobuf/metric/src/main/proto/ai/protomolt/proto/metric/v1/metric.proto)

## protobuf/prompt

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/prompt/InstructionRenderer.java](../../protobuf/prompt/src/main/java/ai/protomolt/proto/prompt/InstructionRenderer.java)
- [java/ai/protomolt/proto/prompt/PromptRenderException.java](../../protobuf/prompt/src/main/java/ai/protomolt/proto/prompt/PromptRenderException.java)
- [java/ai/protomolt/proto/prompt/PromptRenderer.java](../../protobuf/prompt/src/main/java/ai/protomolt/proto/prompt/PromptRenderer.java)
- [java/ai/protomolt/proto/prompt/ResponseFormatShaper.java](../../protobuf/prompt/src/main/java/ai/protomolt/proto/prompt/ResponseFormatShaper.java)
- [java/ai/protomolt/proto/prompt/ViolationFeedbackRenderer.java](../../protobuf/prompt/src/main/java/ai/protomolt/proto/prompt/ViolationFeedbackRenderer.java)
- [java/ai/protomolt/proto/prompt/package-info.java](../../protobuf/prompt/src/main/java/ai/protomolt/proto/prompt/package-info.java)
- [proto/ai/protomolt/proto/prompt/v1/prompt.proto](../../protobuf/prompt/src/main/proto/ai/protomolt/proto/prompt/v1/prompt.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/prompt/InstructionRendererTest.java](../../protobuf/prompt/src/test/java/ai/protomolt/proto/prompt/InstructionRendererTest.java)
- [java/ai/protomolt/proto/prompt/PromptRendererTest.java](../../protobuf/prompt/src/test/java/ai/protomolt/proto/prompt/PromptRendererTest.java)
- [java/ai/protomolt/proto/prompt/ResponseFormatShaperTest.java](../../protobuf/prompt/src/test/java/ai/protomolt/proto/prompt/ResponseFormatShaperTest.java)
- [java/ai/protomolt/proto/prompt/ViolationFeedbackRendererTest.java](../../protobuf/prompt/src/test/java/ai/protomolt/proto/prompt/ViolationFeedbackRendererTest.java)
- [proto/ai/protomolt/proto/prompt/testdata/v1/form.proto](../../protobuf/prompt/src/test/proto/ai/protomolt/proto/prompt/testdata/v1/form.proto)

## protobuf/quality

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/quality/QualityCelFunctions.java](../../protobuf/quality/src/main/java/ai/protomolt/proto/quality/QualityCelFunctions.java)
- [java/ai/protomolt/proto/quality/QualityReport.java](../../protobuf/quality/src/main/java/ai/protomolt/proto/quality/QualityReport.java)
- [java/ai/protomolt/proto/quality/QualitySchemaException.java](../../protobuf/quality/src/main/java/ai/protomolt/proto/quality/QualitySchemaException.java)
- [java/ai/protomolt/proto/quality/QualityScorer.java](../../protobuf/quality/src/main/java/ai/protomolt/proto/quality/QualityScorer.java)
- [java/ai/protomolt/proto/quality/package-info.java](../../protobuf/quality/src/main/java/ai/protomolt/proto/quality/package-info.java)
- [proto/ai/protomolt/proto/quality/v1/quality.proto](../../protobuf/quality/src/main/proto/ai/protomolt/proto/quality/v1/quality.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/quality/QualityScorerTest.java](../../protobuf/quality/src/test/java/ai/protomolt/proto/quality/QualityScorerTest.java)
- [proto/ai/protomolt/proto/quality/testdata/v1/scored_doc.proto](../../protobuf/quality/src/test/proto/ai/protomolt/proto/quality/testdata/v1/scored_doc.proto)

## protobuf/seo

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/seo/SeoIndexing.java](../../protobuf/seo/src/main/java/ai/protomolt/proto/seo/SeoIndexing.java)
- [java/ai/protomolt/proto/seo/package-info.java](../../protobuf/seo/src/main/java/ai/protomolt/proto/seo/package-info.java)
- [proto/ai/protomolt/proto/seo/v1/dublin_core.proto](../../protobuf/seo/src/main/proto/ai/protomolt/proto/seo/v1/dublin_core.proto)
- [proto/ai/protomolt/proto/seo/v1/seo.proto](../../protobuf/seo/src/main/proto/ai/protomolt/proto/seo/v1/seo.proto)
- [proto/ai/protomolt/proto/seo/v1/structured_data.proto](../../protobuf/seo/src/main/proto/ai/protomolt/proto/seo/v1/structured_data.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/seo/SeoIndexMappingTest.java](../../protobuf/seo/src/test/java/ai/protomolt/proto/seo/SeoIndexMappingTest.java)
- [java/ai/protomolt/proto/seo/SeoLuceneEndToEndTest.java](../../protobuf/seo/src/test/java/ai/protomolt/proto/seo/SeoLuceneEndToEndTest.java)
- [java/ai/protomolt/proto/seo/SeoValidationTest.java](../../protobuf/seo/src/test/java/ai/protomolt/proto/seo/SeoValidationTest.java)

## protobuf/types

### Production sources, contracts and SPI registrations

- [proto/ai/protomolt/proto/authz/v1/access_policy.proto](../../protobuf/types/src/main/proto/ai/protomolt/proto/authz/v1/access_policy.proto)
- [proto/ai/protomolt/proto/types/v1/postal.proto](../../protobuf/types/src/main/proto/ai/protomolt/proto/types/v1/postal.proto)
- [proto/ai/protomolt/proto/types/v1/ranges.proto](../../protobuf/types/src/main/proto/ai/protomolt/proto/types/v1/ranges.proto)
- [proto/ai/protomolt/proto/types/v1/screening.proto](../../protobuf/types/src/main/proto/ai/protomolt/proto/types/v1/screening.proto)
- [proto/ai/protomolt/proto/types/v1/taxonomy.proto](../../protobuf/types/src/main/proto/ai/protomolt/proto/types/v1/taxonomy.proto)
- [proto/ai/protomolt/proto/types/v1/tree_path.proto](../../protobuf/types/src/main/proto/ai/protomolt/proto/types/v1/tree_path.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/types/RangeRulesTest.java](../../protobuf/types/src/test/java/ai/protomolt/proto/types/RangeRulesTest.java)
- [java/ai/protomolt/proto/types/TaxonomyRulesTest.java](../../protobuf/types/src/test/java/ai/protomolt/proto/types/TaxonomyRulesTest.java)
- [java/ai/protomolt/proto/types/TreePathRulesTest.java](../../protobuf/types/src/test/java/ai/protomolt/proto/types/TreePathRulesTest.java)

## protobuf/validation

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/validate/CelValues.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/CelValues.java)
- [java/ai/protomolt/proto/validate/ProtoValidator.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/ProtoValidator.java)
- [java/ai/protomolt/proto/validate/RuleCompilationException.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/RuleCompilationException.java)
- [java/ai/protomolt/proto/validate/RuleEvaluationException.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/RuleEvaluationException.java)
- [java/ai/protomolt/proto/validate/ValidationResult.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/ValidationResult.java)
- [java/ai/protomolt/proto/validate/ValueChecks.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/ValueChecks.java)
- [java/ai/protomolt/proto/validate/Violations.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/Violations.java)
- [java/ai/protomolt/proto/validate/WellKnownValues.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/WellKnownValues.java)
- [java/ai/protomolt/proto/validate/cel/ValidationCelFunctions.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/cel/ValidationCelFunctions.java)
- [java/ai/protomolt/proto/validate/cel/package-info.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/cel/package-info.java)
- [java/ai/protomolt/proto/validate/model/AnyConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/AnyConstraints.java)
- [java/ai/protomolt/proto/validate/model/BoolConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/BoolConstraints.java)
- [java/ai/protomolt/proto/validate/model/BytesConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/BytesConstraints.java)
- [java/ai/protomolt/proto/validate/model/BytesFormat.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/BytesFormat.java)
- [java/ai/protomolt/proto/validate/model/CelConstraint.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/CelConstraint.java)
- [java/ai/protomolt/proto/validate/model/DurationConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/DurationConstraints.java)
- [java/ai/protomolt/proto/validate/model/EnumConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/EnumConstraints.java)
- [java/ai/protomolt/proto/validate/model/FieldConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/FieldConstraints.java)
- [java/ai/protomolt/proto/validate/model/FieldMaskConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/FieldMaskConstraints.java)
- [java/ai/protomolt/proto/validate/model/FloatingConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/FloatingConstraints.java)
- [java/ai/protomolt/proto/validate/model/HttpHeaderRule.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/HttpHeaderRule.java)
- [java/ai/protomolt/proto/validate/model/IgnoreMode.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/IgnoreMode.java)
- [java/ai/protomolt/proto/validate/model/IntegralConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/IntegralConstraints.java)
- [java/ai/protomolt/proto/validate/model/MapConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/MapConstraints.java)
- [java/ai/protomolt/proto/validate/model/MessageConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/MessageConstraints.java)
- [java/ai/protomolt/proto/validate/model/RepeatedConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/RepeatedConstraints.java)
- [java/ai/protomolt/proto/validate/model/StringConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/StringConstraints.java)
- [java/ai/protomolt/proto/validate/model/StringFormat.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/StringFormat.java)
- [java/ai/protomolt/proto/validate/model/TimestampConstraints.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/TimestampConstraints.java)
- [java/ai/protomolt/proto/validate/model/package-info.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/model/package-info.java)
- [java/ai/protomolt/proto/validate/package-info.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/package-info.java)
- [java/ai/protomolt/proto/validate/source/GoogleTypeRuleSource.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/source/GoogleTypeRuleSource.java)
- [java/ai/protomolt/proto/validate/source/ProtomoltRuleSource.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/source/ProtomoltRuleSource.java)
- [java/ai/protomolt/proto/validate/source/package-info.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/source/package-info.java)
- [java/ai/protomolt/proto/validate/spi/PostalCodeCatalog.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/spi/PostalCodeCatalog.java)
- [java/ai/protomolt/proto/validate/spi/TaxonomyCatalog.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/spi/TaxonomyCatalog.java)
- [java/ai/protomolt/proto/validate/spi/ValidationRuleSource.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/spi/ValidationRuleSource.java)
- [java/ai/protomolt/proto/validate/spi/ValidationRuleSources.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/spi/ValidationRuleSources.java)
- [java/ai/protomolt/proto/validate/spi/package-info.java](../../protobuf/validation/src/main/java/ai/protomolt/proto/validate/spi/package-info.java)
- [proto/ai/protomolt/proto/validate/v1/validate.proto](../../protobuf/validation/src/main/proto/ai/protomolt/proto/validate/v1/validate.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/validate/BytesRulesExtendedTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/BytesRulesExtendedTest.java)
- [java/ai/protomolt/proto/validate/CelCollectionRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/CelCollectionRulesTest.java)
- [java/ai/protomolt/proto/validate/CelFieldRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/CelFieldRulesTest.java)
- [java/ai/protomolt/proto/validate/CollectionRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/CollectionRulesTest.java)
- [java/ai/protomolt/proto/validate/ExclusiveRangeTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/ExclusiveRangeTest.java)
- [java/ai/protomolt/proto/validate/MapKeyPathTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/MapKeyPathTest.java)
- [java/ai/protomolt/proto/validate/MessageOneofRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/MessageOneofRulesTest.java)
- [java/ai/protomolt/proto/validate/MiscRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/MiscRulesTest.java)
- [java/ai/protomolt/proto/validate/NumericRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/NumericRulesTest.java)
- [java/ai/protomolt/proto/validate/PostalCodeRuleTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/PostalCodeRuleTest.java)
- [java/ai/protomolt/proto/validate/PresenceAndIgnoreTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/PresenceAndIgnoreTest.java)
- [java/ai/protomolt/proto/validate/ProtoValidatorTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/ProtoValidatorTest.java)
- [java/ai/protomolt/proto/validate/ProtomoltRuleSourceTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/ProtomoltRuleSourceTest.java)
- [java/ai/protomolt/proto/validate/RepeatedUniqueTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/RepeatedUniqueTest.java)
- [java/ai/protomolt/proto/validate/RuleClassificationTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/RuleClassificationTest.java)
- [java/ai/protomolt/proto/validate/RuleSourceSpiTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/RuleSourceSpiTest.java)
- [java/ai/protomolt/proto/validate/SkipWhenTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/SkipWhenTest.java)
- [java/ai/protomolt/proto/validate/SlugAndRegionRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/SlugAndRegionRulesTest.java)
- [java/ai/protomolt/proto/validate/StringRulesExtendedTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/StringRulesExtendedTest.java)
- [java/ai/protomolt/proto/validate/StringRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/StringRulesTest.java)
- [java/ai/protomolt/proto/validate/TaxonomyRuleTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/TaxonomyRuleTest.java)
- [java/ai/protomolt/proto/validate/TimeEdgeCasesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/TimeEdgeCasesTest.java)
- [java/ai/protomolt/proto/validate/TimeRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/TimeRulesTest.java)
- [java/ai/protomolt/proto/validate/UnregisteredExtensionsTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/UnregisteredExtensionsTest.java)
- [java/ai/protomolt/proto/validate/ValidationInstantTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/ValidationInstantTest.java)
- [java/ai/protomolt/proto/validate/ValidationResultTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/ValidationResultTest.java)
- [java/ai/protomolt/proto/validate/ValidatorConstructionTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/ValidatorConstructionTest.java)
- [java/ai/protomolt/proto/validate/WellKnownFormatRulesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/WellKnownFormatRulesTest.java)
- [java/ai/protomolt/proto/validate/WellKnownTypesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/WellKnownTypesTest.java)
- [java/ai/protomolt/proto/validate/cel/FormatFunctionTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/cel/FormatFunctionTest.java)
- [java/ai/protomolt/proto/validate/cel/ValidationCelFunctionsTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/cel/ValidationCelFunctionsTest.java)
- [java/ai/protomolt/proto/validate/model/ConstraintModelTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/model/ConstraintModelTest.java)
- [java/ai/protomolt/proto/validate/model/FormatEnumsTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/model/FormatEnumsTest.java)
- [java/ai/protomolt/proto/validate/model/HttpHeaderRuleTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/model/HttpHeaderRuleTest.java)
- [java/ai/protomolt/proto/validate/source/GoogleTypeRuleSourceTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/source/GoogleTypeRuleSourceTest.java)
- [java/ai/protomolt/proto/validate/spi/ValidationRuleSourcesTest.java](../../protobuf/validation/src/test/java/ai/protomolt/proto/validate/spi/ValidationRuleSourcesTest.java)
- [proto/ai/protomolt/proto/validate/testdata/v1/container.proto](../../protobuf/validation/src/test/proto/ai/protomolt/proto/validate/testdata/v1/container.proto)
- [proto/ai/protomolt/proto/validate/testdata/v1/draft.proto](../../protobuf/validation/src/test/proto/ai/protomolt/proto/validate/testdata/v1/draft.proto)
- [proto/ai/protomolt/proto/validate/testdata/v1/extra.proto](../../protobuf/validation/src/test/proto/ai/protomolt/proto/validate/testdata/v1/extra.proto)
- [proto/ai/protomolt/proto/validate/testdata/v1/gauntlet.proto](../../protobuf/validation/src/test/proto/ai/protomolt/proto/validate/testdata/v1/gauntlet.proto)
- [proto/ai/protomolt/proto/validate/testdata/v1/person.proto](../../protobuf/validation/src/test/proto/ai/protomolt/proto/validate/testdata/v1/person.proto)
- [proto/ai/protomolt/proto/validate/testdata/v1/well_known.proto](../../protobuf/validation/src/test/proto/ai/protomolt/proto/validate/testdata/v1/well_known.proto)

## protobuf/validation-conformance

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/validate/conformance/ConformanceMain.java](../../protobuf/validation-conformance/src/main/java/ai/protomolt/proto/validate/conformance/ConformanceMain.java)
- [java/ai/protomolt/proto/validate/conformance/ConformanceRunner.java](../../protobuf/validation-conformance/src/main/java/ai/protomolt/proto/validate/conformance/ConformanceRunner.java)
- [java/ai/protomolt/proto/validate/conformance/DescriptorSets.java](../../protobuf/validation-conformance/src/main/java/ai/protomolt/proto/validate/conformance/DescriptorSets.java)
- [java/ai/protomolt/proto/validate/conformance/FieldPaths.java](../../protobuf/validation-conformance/src/main/java/ai/protomolt/proto/validate/conformance/FieldPaths.java)
- [java/ai/protomolt/proto/validate/conformance/package-info.java](../../protobuf/validation-conformance/src/main/java/ai/protomolt/proto/validate/conformance/package-info.java)
- [proto/buf/validate/conformance/harness/harness.proto](../../protobuf/validation-conformance/src/main/proto/buf/validate/conformance/harness/harness.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/validate/conformance/ConformanceHarnessTest.java](../../protobuf/validation-conformance/src/test/java/ai/protomolt/proto/validate/conformance/ConformanceHarnessTest.java)
- [java/ai/protomolt/proto/validate/conformance/ConformanceMainTest.java](../../protobuf/validation-conformance/src/test/java/ai/protomolt/proto/validate/conformance/ConformanceMainTest.java)
- [java/ai/protomolt/proto/validate/conformance/FieldPathsTest.java](../../protobuf/validation-conformance/src/test/java/ai/protomolt/proto/validate/conformance/FieldPathsTest.java)
- [proto/ai/protomolt/proto/validate/conformance/testdata/v1/cases.proto](../../protobuf/validation-conformance/src/test/proto/ai/protomolt/proto/validate/conformance/testdata/v1/cases.proto)

## protobuf/validation-protovalidate

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/validate/protovalidate/FieldRuleTranslation.java](../../protobuf/validation-protovalidate/src/main/java/ai/protomolt/proto/validate/protovalidate/FieldRuleTranslation.java)
- [java/ai/protomolt/proto/validate/protovalidate/OptionReparse.java](../../protobuf/validation-protovalidate/src/main/java/ai/protomolt/proto/validate/protovalidate/OptionReparse.java)
- [java/ai/protomolt/proto/validate/protovalidate/PredefinedIndex.java](../../protobuf/validation-protovalidate/src/main/java/ai/protomolt/proto/validate/protovalidate/PredefinedIndex.java)
- [java/ai/protomolt/proto/validate/protovalidate/PredefinedRules.java](../../protobuf/validation-protovalidate/src/main/java/ai/protomolt/proto/validate/protovalidate/PredefinedRules.java)
- [java/ai/protomolt/proto/validate/protovalidate/ProtovalidateRuleSource.java](../../protobuf/validation-protovalidate/src/main/java/ai/protomolt/proto/validate/protovalidate/ProtovalidateRuleSource.java)
- [java/ai/protomolt/proto/validate/protovalidate/RulePayloads.java](../../protobuf/validation-protovalidate/src/main/java/ai/protomolt/proto/validate/protovalidate/RulePayloads.java)
- [java/ai/protomolt/proto/validate/protovalidate/package-info.java](../../protobuf/validation-protovalidate/src/main/java/ai/protomolt/proto/validate/protovalidate/package-info.java)
- [proto/buf/validate/validate.proto](../../protobuf/validation-protovalidate/src/main/proto/buf/validate/validate.proto)
- [resources/META-INF/services/ai.protomolt.proto.validate.spi.ValidationRuleSource](../../protobuf/validation-protovalidate/src/main/resources/META-INF/services/ai.protomolt.proto.validate.spi.ValidationRuleSource)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/validate/protovalidate/PredefinedRulesTest.java](../../protobuf/validation-protovalidate/src/test/java/ai/protomolt/proto/validate/protovalidate/PredefinedRulesTest.java)
- [java/ai/protomolt/proto/validate/protovalidate/ProtovalidateRuleSourceTest.java](../../protobuf/validation-protovalidate/src/test/java/ai/protomolt/proto/validate/protovalidate/ProtovalidateRuleSourceTest.java)
- [java/ai/protomolt/proto/validate/protovalidate/RetainedDescriptorValidationTest.java](../../protobuf/validation-protovalidate/src/test/java/ai/protomolt/proto/validate/protovalidate/RetainedDescriptorValidationTest.java)
- [java/ai/protomolt/proto/validate/protovalidate/SchemaPreparationTest.java](../../protobuf/validation-protovalidate/src/test/java/ai/protomolt/proto/validate/protovalidate/SchemaPreparationTest.java)
- [java/ai/protomolt/proto/validate/protovalidate/UnknownFieldFallbackTest.java](../../protobuf/validation-protovalidate/src/test/java/ai/protomolt/proto/validate/protovalidate/UnknownFieldFallbackTest.java)
- [proto/ai/protomolt/proto/validate/protovalidate/testdata/v1/predefined_ext.proto](../../protobuf/validation-protovalidate/src/test/proto/ai/protomolt/proto/validate/protovalidate/testdata/v1/predefined_ext.proto)
- [proto/ai/protomolt/proto/validate/protovalidate/testdata/v1/predefined_rules.proto](../../protobuf/validation-protovalidate/src/test/proto/ai/protomolt/proto/validate/protovalidate/testdata/v1/predefined_rules.proto)
- [proto/ai/protomolt/proto/validate/protovalidate/testdata/v1/protovalidate_annotated.proto](../../protobuf/validation-protovalidate/src/test/proto/ai/protomolt/proto/validate/protovalidate/testdata/v1/protovalidate_annotated.proto)

## repo/admission

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/repo/admission/BuiltinDocumentSchema.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/BuiltinDocumentSchema.java)
- [java/ai/protomolt/proto/repo/admission/DocumentAdmissionPolicy.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentAdmissionPolicy.java)
- [java/ai/protomolt/proto/repo/admission/DocumentAdmissionReservations.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentAdmissionReservations.java)
- [java/ai/protomolt/proto/repo/admission/DocumentAdmissionResources.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentAdmissionResources.java)
- [java/ai/protomolt/proto/repo/admission/DocumentAnyRootInventory.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentAnyRootInventory.java)
- [java/ai/protomolt/proto/repo/admission/DocumentCompositeSchemaResolution.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentCompositeSchemaResolution.java)
- [java/ai/protomolt/proto/repo/admission/DocumentFragmentSchemaReplay.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentFragmentSchemaReplay.java)
- [java/ai/protomolt/proto/repo/admission/DocumentPayloadAssessment.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentPayloadAssessment.java)
- [java/ai/protomolt/proto/repo/admission/DocumentPayloadCheck.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentPayloadCheck.java)
- [java/ai/protomolt/proto/repo/admission/DocumentRetainedSchemaAssets.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentRetainedSchemaAssets.java)
- [java/ai/protomolt/proto/repo/admission/DocumentRetainedSchemaResolution.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentRetainedSchemaResolution.java)
- [java/ai/protomolt/proto/repo/admission/DocumentRootSchemaEvidenceCodec.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentRootSchemaEvidenceCodec.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAdmission.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaAdmission.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaArtifactCache.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaArtifactCache.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAssessment.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaAssessment.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAssessmentReplay.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaAssessmentReplay.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAssetBinding.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaAssetBinding.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAssetCodec.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaAssetCodec.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaBinding.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaBinding.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaDefinitions.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaDefinitions.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaEvidenceCodec.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaEvidenceCodec.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaOccurrenceCodec.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaOccurrenceCodec.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaOccurrenceProjection.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaOccurrenceProjection.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaOccurrences.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaOccurrences.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaPreparation.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaPreparation.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaReplay.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaReplay.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaRootCodec.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaRootCodec.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaRootProjection.java](../../repo/admission/src/main/java/ai/protomolt/proto/repo/admission/DocumentSchemaRootProjection.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/repo/admission/BuiltinDocumentSchemaTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/BuiltinDocumentSchemaTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentAdmissionPolicyTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentAdmissionPolicyTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentAnyRootInventoryTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentAnyRootInventoryTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentFragmentSchemaReplayTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentFragmentSchemaReplayTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentPayloadAssessmentTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentPayloadAssessmentTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentPayloadCheckTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentPayloadCheckTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentRetainedSchemaAssetsTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentRetainedSchemaAssetsTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentRetainedSchemaReferenceTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentRetainedSchemaReferenceTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentRootSchemaEvidenceCodecTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentRootSchemaEvidenceCodecTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAdmissionTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaAdmissionTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaArtifactCacheTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaArtifactCacheTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAssessmentReplayTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaAssessmentReplayTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAssessmentTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaAssessmentTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaAssetCodecTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaAssetCodecTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaBindingTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaBindingTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaOccurrenceCodecTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaOccurrenceCodecTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaPreparationTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaPreparationTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaReferenceTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaReferenceTest.java)
- [java/ai/protomolt/proto/repo/admission/DocumentSchemaRootCodecTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/DocumentSchemaRootCodecTest.java)
- [java/ai/protomolt/proto/repo/admission/MapOccurrenceSemanticsTest.java](../../repo/admission/src/test/java/ai/protomolt/proto/repo/admission/MapOccurrenceSemanticsTest.java)

## repo/blob/cache

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/repo/blob/cache/CachingBlobStore.java](../../repo/blob/cache/src/main/java/ai/protomolt/proto/repo/blob/cache/CachingBlobStore.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/repo/blob/cache/CacheCloseTest.java](../../repo/blob/cache/src/test/java/ai/protomolt/proto/repo/blob/cache/CacheCloseTest.java)
- [java/ai/protomolt/proto/repo/blob/cache/CacheProviderIT.java](../../repo/blob/cache/src/test/java/ai/protomolt/proto/repo/blob/cache/CacheProviderIT.java)

## repo/blob/redis

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/repo/blob/redis/RedisBackendIdentity.java](../../repo/blob/redis/src/main/java/ai/protomolt/proto/repo/blob/redis/RedisBackendIdentity.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisBlobStore.java](../../repo/blob/redis/src/main/java/ai/protomolt/proto/repo/blob/redis/RedisBlobStore.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisBlobStoreConfig.java](../../repo/blob/redis/src/main/java/ai/protomolt/proto/repo/blob/redis/RedisBlobStoreConfig.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisBlobStoreProvider.java](../../repo/blob/redis/src/main/java/ai/protomolt/proto/repo/blob/redis/RedisBlobStoreProvider.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisObjectKeys.java](../../repo/blob/redis/src/main/java/ai/protomolt/proto/repo/blob/redis/RedisObjectKeys.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisObjectScripts.java](../../repo/blob/redis/src/main/java/ai/protomolt/proto/repo/blob/redis/RedisObjectScripts.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisWriteMetadata.java](../../repo/blob/redis/src/main/java/ai/protomolt/proto/repo/blob/redis/RedisWriteMetadata.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisWritePolicy.java](../../repo/blob/redis/src/main/java/ai/protomolt/proto/repo/blob/redis/RedisWritePolicy.java)
- [resources/META-INF/services/ai.protomolt.proto.repo.blob.spi.BlobStoreProvider](../../repo/blob/redis/src/main/resources/META-INF/services/ai.protomolt.proto.repo.blob.spi.BlobStoreProvider)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/repo/blob/redis/RedisAtomicStorageIT.java](../../repo/blob/redis/src/test/java/ai/protomolt/proto/repo/blob/redis/RedisAtomicStorageIT.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisBackendIdentityTest.java](../../repo/blob/redis/src/test/java/ai/protomolt/proto/repo/blob/redis/RedisBackendIdentityTest.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisBlobStoreConfigTest.java](../../repo/blob/redis/src/test/java/ai/protomolt/proto/repo/blob/redis/RedisBlobStoreConfigTest.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisBlobStoreIT.java](../../repo/blob/redis/src/test/java/ai/protomolt/proto/repo/blob/redis/RedisBlobStoreIT.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisConditionalWriteIT.java](../../repo/blob/redis/src/test/java/ai/protomolt/proto/repo/blob/redis/RedisConditionalWriteIT.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisCreateOnlyIT.java](../../repo/blob/redis/src/test/java/ai/protomolt/proto/repo/blob/redis/RedisCreateOnlyIT.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisPersistenceIT.java](../../repo/blob/redis/src/test/java/ai/protomolt/proto/repo/blob/redis/RedisPersistenceIT.java)
- [java/ai/protomolt/proto/repo/blob/redis/RedisProviderTest.java](../../repo/blob/redis/src/test/java/ai/protomolt/proto/repo/blob/redis/RedisProviderTest.java)

## repo/blob/s3

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/repo/blob/s3/S3BackendIdentity.java](../../repo/blob/s3/src/main/java/ai/protomolt/proto/repo/blob/s3/S3BackendIdentity.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3BlobStore.java](../../repo/blob/s3/src/main/java/ai/protomolt/proto/repo/blob/s3/S3BlobStore.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3BlobStoreProvider.java](../../repo/blob/s3/src/main/java/ai/protomolt/proto/repo/blob/s3/S3BlobStoreProvider.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ExactVersionReclaimer.java](../../repo/blob/s3/src/main/java/ai/protomolt/proto/repo/blob/s3/S3ExactVersionReclaimer.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3NamespaceProvisioner.java](../../repo/blob/s3/src/main/java/ai/protomolt/proto/repo/blob/s3/S3NamespaceProvisioner.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ObjectReads.java](../../repo/blob/s3/src/main/java/ai/protomolt/proto/repo/blob/s3/S3ObjectReads.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ObjectReclaimer.java](../../repo/blob/s3/src/main/java/ai/protomolt/proto/repo/blob/s3/S3ObjectReclaimer.java)
- [resources/META-INF/services/ai.protomolt.proto.repo.blob.spi.BlobStoreProvider](../../repo/blob/s3/src/main/resources/META-INF/services/ai.protomolt.proto.repo.blob.spi.BlobStoreProvider)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/repo/blob/s3/RustFsConditionalBlobStoreIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/RustFsConditionalBlobStoreIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/RustFsDamagedObjectReadIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/RustFsDamagedObjectReadIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/RustFsExactVersionReclaimerIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/RustFsExactVersionReclaimerIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3BackendIdentityTest.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3BackendIdentityTest.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3BlobStoreIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3BlobStoreIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3BoundedReadFailureTest.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3BoundedReadFailureTest.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3BoundedReadIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3BoundedReadIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3BoundedReadStreamTest.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3BoundedReadStreamTest.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ConditionalReadBoundTest.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3ConditionalReadBoundTest.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ExactVersionRaceIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3ExactVersionRaceIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ExactVersionReclaimerIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3ExactVersionReclaimerIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ExactVersionReclaimerTest.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3ExactVersionReclaimerTest.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3LocalStackConditionalIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3LocalStackConditionalIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3NamespaceProvisionerIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3NamespaceProvisionerIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ObjectReclaimerIT.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3ObjectReclaimerIT.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ProviderLifecycleTest.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3ProviderLifecycleTest.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ProviderTest.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3ProviderTest.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3ReadFailureTest.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3ReadFailureTest.java)
- [java/ai/protomolt/proto/repo/blob/s3/S3RequestRelay.java](../../repo/blob/s3/src/test/java/ai/protomolt/proto/repo/blob/s3/S3RequestRelay.java)

## repo/blob/spi

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/repo/blob/spi/BackendIdentity.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/BackendIdentity.java)
- [java/ai/protomolt/proto/repo/blob/spi/BlobCapability.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/BlobCapability.java)
- [java/ai/protomolt/proto/repo/blob/spi/BlobStore.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/BlobStore.java)
- [java/ai/protomolt/proto/repo/blob/spi/BlobStoreException.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/BlobStoreException.java)
- [java/ai/protomolt/proto/repo/blob/spi/BlobStoreProvider.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/BlobStoreProvider.java)
- [java/ai/protomolt/proto/repo/blob/spi/BlobStores.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/BlobStores.java)
- [java/ai/protomolt/proto/repo/blob/spi/ExactVersionOutcome.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/ExactVersionOutcome.java)
- [java/ai/protomolt/proto/repo/blob/spi/ExactVersionReclaimer.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/ExactVersionReclaimer.java)
- [java/ai/protomolt/proto/repo/blob/spi/ExpiringBlobStore.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/ExpiringBlobStore.java)
- [java/ai/protomolt/proto/repo/blob/spi/NamespaceProvisioner.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/NamespaceProvisioner.java)
- [java/ai/protomolt/proto/repo/blob/spi/ObjectReclaimer.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/ObjectReclaimer.java)
- [java/ai/protomolt/proto/repo/blob/spi/OpenedBlobStore.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/OpenedBlobStore.java)
- [java/ai/protomolt/proto/repo/blob/spi/PayloadBudget.java](../../repo/blob/spi/src/main/java/ai/protomolt/proto/repo/blob/spi/PayloadBudget.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/repo/blob/spi/BlobStoresTest.java](../../repo/blob/spi/src/test/java/ai/protomolt/proto/repo/blob/spi/BlobStoresTest.java)
- [java/ai/protomolt/proto/repo/blob/spi/BoundedReadDefaultTest.java](../../repo/blob/spi/src/test/java/ai/protomolt/proto/repo/blob/spi/BoundedReadDefaultTest.java)
- [java/ai/protomolt/proto/repo/blob/spi/OpenedBlobStoreExactVersionTest.java](../../repo/blob/spi/src/test/java/ai/protomolt/proto/repo/blob/spi/OpenedBlobStoreExactVersionTest.java)
- [java/ai/protomolt/proto/repo/blob/spi/PayloadBudgetTest.java](../../repo/blob/spi/src/test/java/ai/protomolt/proto/repo/blob/spi/PayloadBudgetTest.java)

## repo/codec

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/repo/codec/ChunkRunNames.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/ChunkRunNames.java)
- [java/ai/protomolt/proto/repo/codec/DocumentChunkSequence.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/DocumentChunkSequence.java)
- [java/ai/protomolt/proto/repo/codec/DocumentFragmentConfinement.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/DocumentFragmentConfinement.java)
- [java/ai/protomolt/proto/repo/codec/DocumentPartCodec.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/DocumentPartCodec.java)
- [java/ai/protomolt/proto/repo/codec/DocumentRevisionAssembly.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/DocumentRevisionAssembly.java)
- [java/ai/protomolt/proto/repo/codec/PartLayout.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/PartLayout.java)
- [java/ai/protomolt/proto/repo/codec/PartLayouts.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/PartLayouts.java)
- [java/ai/protomolt/proto/repo/codec/PartObject.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/PartObject.java)
- [java/ai/protomolt/proto/repo/codec/RepositoryNamespaces.java](../../repo/codec/src/main/java/ai/protomolt/proto/repo/codec/RepositoryNamespaces.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/repo/codec/DocumentChunkSequenceTest.java](../../repo/codec/src/test/java/ai/protomolt/proto/repo/codec/DocumentChunkSequenceTest.java)
- [java/ai/protomolt/proto/repo/codec/DocumentFragmentConfinementTest.java](../../repo/codec/src/test/java/ai/protomolt/proto/repo/codec/DocumentFragmentConfinementTest.java)
- [java/ai/protomolt/proto/repo/codec/DocumentPartCodecEdgeTest.java](../../repo/codec/src/test/java/ai/protomolt/proto/repo/codec/DocumentPartCodecEdgeTest.java)
- [java/ai/protomolt/proto/repo/codec/DocumentPartCodecTest.java](../../repo/codec/src/test/java/ai/protomolt/proto/repo/codec/DocumentPartCodecTest.java)
- [java/ai/protomolt/proto/repo/codec/DocumentRevisionAssemblyTest.java](../../repo/codec/src/test/java/ai/protomolt/proto/repo/codec/DocumentRevisionAssemblyTest.java)
- [java/ai/protomolt/proto/repo/codec/DocumentWireBudgetTest.java](../../repo/codec/src/test/java/ai/protomolt/proto/repo/codec/DocumentWireBudgetTest.java)

## repo/schema/registry

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/repo/schema/registry/RegistryDescriptorLoads.java](../../repo/schema/registry/src/main/java/ai/protomolt/proto/repo/schema/registry/RegistryDescriptorLoads.java)
- [java/ai/protomolt/proto/repo/schema/registry/RegistrySchemaResolver.java](../../repo/schema/registry/src/main/java/ai/protomolt/proto/repo/schema/registry/RegistrySchemaResolver.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/repo/schema/registry/RegistryDescriptorLoadsTest.java](../../repo/schema/registry/src/test/java/ai/protomolt/proto/repo/schema/registry/RegistryDescriptorLoadsTest.java)
- [java/ai/protomolt/proto/repo/schema/registry/RegistrySchemaResolverTest.java](../../repo/schema/registry/src/test/java/ai/protomolt/proto/repo/schema/registry/RegistrySchemaResolverTest.java)

## schema/apicurio/deployment

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/schema/apicurio/deployment/ProtoToolsApicurioProcessor.java](../../schema/apicurio/deployment/src/main/java/ai/protomolt/proto/schema/apicurio/deployment/ProtoToolsApicurioProcessor.java)
- [java/ai/protomolt/proto/schema/apicurio/deployment/package-info.java](../../schema/apicurio/deployment/src/main/java/ai/protomolt/proto/schema/apicurio/deployment/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/schema/apicurio/deployment/ProtoToolsApicurioProcessorTest.java](../../schema/apicurio/deployment/src/test/java/ai/protomolt/proto/schema/apicurio/deployment/ProtoToolsApicurioProcessorTest.java)
- [java/ai/protomolt/proto/schema/apicurio/deployment/ProtoToolsApicurioProcessorWiringTest.java](../../schema/apicurio/deployment/src/test/java/ai/protomolt/proto/schema/apicurio/deployment/ProtoToolsApicurioProcessorWiringTest.java)

## schema/apicurio/runtime

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorInstaller.java](../../schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorInstaller.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoader.java](../../schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoader.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderProducer.java](../../schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderProducer.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioProtobufParseFallback.java](../../schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/ApicurioProtobufParseFallback.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioReferenceResolver.java](../../schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/ApicurioReferenceResolver.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioSchemaPublisher.java](../../schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/ApicurioSchemaPublisher.java)
- [java/ai/protomolt/proto/schema/apicurio/ProtoToolsApicurioConfig.java](../../schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/ProtoToolsApicurioConfig.java)
- [java/ai/protomolt/proto/schema/apicurio/package-info.java](../../schema/apicurio/runtime/src/main/java/ai/protomolt/proto/schema/apicurio/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorInstallerTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorInstallerTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderBulkTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderBulkTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderIntegrationTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderIntegrationTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderProducerTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderProducerTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioDescriptorLoaderTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioProtobufParseFallbackEdgeCasesTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioProtobufParseFallbackEdgeCasesTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioProtobufParseFallbackTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioProtobufParseFallbackTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioReferenceResolverTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioReferenceResolverTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioRegistryContainer.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioRegistryContainer.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioRegistryContainerLogIT.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioRegistryContainerLogIT.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioSchemaPublisherClassificationTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioSchemaPublisherClassificationTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioSchemaPublisherIntegrationTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioSchemaPublisherIntegrationTest.java)
- [java/ai/protomolt/proto/schema/apicurio/ApicurioSchemaPublisherTest.java](../../schema/apicurio/runtime/src/test/java/ai/protomolt/proto/schema/apicurio/ApicurioSchemaPublisherTest.java)

## schema/confluent

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/schema/confluent/ConfluentDescriptorSource.java](../../schema/confluent/src/main/java/ai/protomolt/proto/schema/confluent/ConfluentDescriptorSource.java)
- [java/ai/protomolt/proto/schema/confluent/ConfluentSchemaPublisher.java](../../schema/confluent/src/main/java/ai/protomolt/proto/schema/confluent/ConfluentSchemaPublisher.java)
- [java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryLoader.java](../../schema/confluent/src/main/java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryLoader.java)
- [java/ai/protomolt/proto/schema/confluent/package-info.java](../../schema/confluent/src/main/java/ai/protomolt/proto/schema/confluent/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/schema/confluent/AbstractConfluentCompatIntegrationTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/AbstractConfluentCompatIntegrationTest.java)
- [java/ai/protomolt/proto/schema/confluent/AbstractConfluentPublisherIntegrationTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/AbstractConfluentPublisherIntegrationTest.java)
- [java/ai/protomolt/proto/schema/confluent/ApicurioCcompatIntegrationTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/ApicurioCcompatIntegrationTest.java)
- [java/ai/protomolt/proto/schema/confluent/ApicurioCcompatPublisherIntegrationTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/ApicurioCcompatPublisherIntegrationTest.java)
- [java/ai/protomolt/proto/schema/confluent/ApicurioRegistryContainer.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/ApicurioRegistryContainer.java)
- [java/ai/protomolt/proto/schema/confluent/ConfluentDescriptorSourceTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/ConfluentDescriptorSourceTest.java)
- [java/ai/protomolt/proto/schema/confluent/ConfluentSchemaPublisherTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/ConfluentSchemaPublisherTest.java)
- [java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryIntegrationTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryIntegrationTest.java)
- [java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryLoaderTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryLoaderTest.java)
- [java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryPublisherIntegrationTest.java](../../schema/confluent/src/test/java/ai/protomolt/proto/schema/confluent/ConfluentSchemaRegistryPublisherIntegrationTest.java)

## schema/registry/composer

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/schema/registry/composer/SchemaRegistryModule.java](../../schema/registry/composer/src/main/java/ai/protomolt/proto/schema/registry/composer/SchemaRegistryModule.java)
- [resources/META-INF/services/ai.protomolt.proto.composer.ServiceModule](../../schema/registry/composer/src/main/resources/META-INF/services/ai.protomolt.proto.composer.ServiceModule)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/schema/registry/composer/SchemaRegistryModuleTest.java](../../schema/registry/composer/src/test/java/ai/protomolt/proto/schema/registry/composer/SchemaRegistryModuleTest.java)

## schema/registry/core

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/registry/CompatibilityModes.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/CompatibilityModes.java)
- [java/ai/protomolt/proto/registry/CompatibilityWriteGate.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/CompatibilityWriteGate.java)
- [java/ai/protomolt/proto/registry/ConfigDocumentStore.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/ConfigDocumentStore.java)
- [java/ai/protomolt/proto/registry/ConfigSupport.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/ConfigSupport.java)
- [java/ai/protomolt/proto/registry/DescriptorSetArtifacts.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/DescriptorSetArtifacts.java)
- [java/ai/protomolt/proto/registry/InMemorySchemaRegistryStore.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/InMemorySchemaRegistryStore.java)
- [java/ai/protomolt/proto/registry/InMemorySchemaRegistryStoreProvider.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/InMemorySchemaRegistryStoreProvider.java)
- [java/ai/protomolt/proto/registry/IncompatibleRegistrationException.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/IncompatibleRegistrationException.java)
- [java/ai/protomolt/proto/registry/InvalidConfigException.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/InvalidConfigException.java)
- [java/ai/protomolt/proto/registry/InvalidSchemaException.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/InvalidSchemaException.java)
- [java/ai/protomolt/proto/registry/ReferenceConflictException.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/ReferenceConflictException.java)
- [java/ai/protomolt/proto/registry/ReferenceNotFoundException.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/ReferenceNotFoundException.java)
- [java/ai/protomolt/proto/registry/RegistrationSupport.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/RegistrationSupport.java)
- [java/ai/protomolt/proto/registry/RegistryStoreException.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/RegistryStoreException.java)
- [java/ai/protomolt/proto/registry/SchemaContents.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/SchemaContents.java)
- [java/ai/protomolt/proto/registry/SchemaReference.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/SchemaReference.java)
- [java/ai/protomolt/proto/registry/SchemaRegistryStore.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/SchemaRegistryStore.java)
- [java/ai/protomolt/proto/registry/SchemaRegistryStoreProvider.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/SchemaRegistryStoreProvider.java)
- [java/ai/protomolt/proto/registry/SchemaRegistryStores.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/SchemaRegistryStores.java)
- [java/ai/protomolt/proto/registry/StoredSchema.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/StoredSchema.java)
- [java/ai/protomolt/proto/registry/StoredSchemaSources.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/StoredSchemaSources.java)
- [java/ai/protomolt/proto/registry/WorkflowDocumentStore.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/WorkflowDocumentStore.java)
- [java/ai/protomolt/proto/registry/package-info.java](../../schema/registry/core/src/main/java/ai/protomolt/proto/registry/package-info.java)
- [resources/META-INF/services/ai.protomolt.proto.registry.SchemaRegistryStoreProvider](../../schema/registry/core/src/main/resources/META-INF/services/ai.protomolt.proto.registry.SchemaRegistryStoreProvider)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/registry/CompatibilityWriteGateTest.java](../../schema/registry/core/src/test/java/ai/protomolt/proto/registry/CompatibilityWriteGateTest.java)
- [java/ai/protomolt/proto/registry/ConfigSupportTest.java](../../schema/registry/core/src/test/java/ai/protomolt/proto/registry/ConfigSupportTest.java)
- [java/ai/protomolt/proto/registry/InMemorySchemaRegistryStoreTest.java](../../schema/registry/core/src/test/java/ai/protomolt/proto/registry/InMemorySchemaRegistryStoreTest.java)
- [java/ai/protomolt/proto/registry/RegistrationHistoryTest.java](../../schema/registry/core/src/test/java/ai/protomolt/proto/registry/RegistrationHistoryTest.java)
- [java/ai/protomolt/proto/registry/SchemaReferenceTest.java](../../schema/registry/core/src/test/java/ai/protomolt/proto/registry/SchemaReferenceTest.java)
- [java/ai/protomolt/proto/registry/SchemaRegistryStoresTest.java](../../schema/registry/core/src/test/java/ai/protomolt/proto/registry/SchemaRegistryStoresTest.java)
- [java/ai/protomolt/proto/registry/StoredSchemaSourcesTest.java](../../schema/registry/core/src/test/java/ai/protomolt/proto/registry/StoredSchemaSourcesTest.java)

## schema/registry/git

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/schema/registry/git/GitSchemaRegistryStore.java](../../schema/registry/git/src/main/java/ai/protomolt/proto/schema/registry/git/GitSchemaRegistryStore.java)
- [java/ai/protomolt/proto/schema/registry/git/GitSchemaRegistryStoreProvider.java](../../schema/registry/git/src/main/java/ai/protomolt/proto/schema/registry/git/GitSchemaRegistryStoreProvider.java)
- [java/ai/protomolt/proto/schema/registry/git/RegistryFederation.java](../../schema/registry/git/src/main/java/ai/protomolt/proto/schema/registry/git/RegistryFederation.java)
- [java/ai/protomolt/proto/schema/registry/git/RegistryWorkflowVersionRepository.java](../../schema/registry/git/src/main/java/ai/protomolt/proto/schema/registry/git/RegistryWorkflowVersionRepository.java)
- [resources/META-INF/services/ai.protomolt.proto.registry.SchemaRegistryStoreProvider](../../schema/registry/git/src/main/resources/META-INF/services/ai.protomolt.proto.registry.SchemaRegistryStoreProvider)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/schema/registry/git/GitSchemaRegistryStoreProviderTest.java](../../schema/registry/git/src/test/java/ai/protomolt/proto/schema/registry/git/GitSchemaRegistryStoreProviderTest.java)
- [java/ai/protomolt/proto/schema/registry/git/GitSchemaRegistryStoreTest.java](../../schema/registry/git/src/test/java/ai/protomolt/proto/schema/registry/git/GitSchemaRegistryStoreTest.java)
- [java/ai/protomolt/proto/schema/registry/git/RegistrationSupportTest.java](../../schema/registry/git/src/test/java/ai/protomolt/proto/schema/registry/git/RegistrationSupportTest.java)
- [java/ai/protomolt/proto/schema/registry/git/RegistryFederationTest.java](../../schema/registry/git/src/test/java/ai/protomolt/proto/schema/registry/git/RegistryFederationTest.java)
- [java/ai/protomolt/proto/schema/registry/git/SubjectPathTraversalTest.java](../../schema/registry/git/src/test/java/ai/protomolt/proto/schema/registry/git/SubjectPathTraversalTest.java)
- [java/ai/protomolt/proto/schema/registry/git/WorkflowPromotionTest.java](../../schema/registry/git/src/test/java/ai/protomolt/proto/schema/registry/git/WorkflowPromotionTest.java)

## schema/registry/git/service

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/schema/registry/git/service/RegistryModule.java](../../schema/registry/git/service/src/main/java/ai/protomolt/proto/schema/registry/git/service/RegistryModule.java)
- [java/ai/protomolt/proto/schema/registry/git/service/RegistryRemotesAction.java](../../schema/registry/git/service/src/main/java/ai/protomolt/proto/schema/registry/git/service/RegistryRemotesAction.java)
- [java/ai/protomolt/proto/schema/registry/git/service/RegistrySyncAction.java](../../schema/registry/git/service/src/main/java/ai/protomolt/proto/schema/registry/git/service/RegistrySyncAction.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/schema/registry/git/service/FederationActionsTest.java](../../schema/registry/git/service/src/test/java/ai/protomolt/proto/schema/registry/git/service/FederationActionsTest.java)
- [java/ai/protomolt/proto/schema/registry/git/service/RegistryWorkflowRepositoryTest.java](../../schema/registry/git/service/src/test/java/ai/protomolt/proto/schema/registry/git/service/RegistryWorkflowRepositoryTest.java)

## schema/registry/proto

### Production sources, contracts and SPI registrations

- [proto/ai/protomolt/proto/schema/registry/v1/descriptor_exchange.proto](../../schema/registry/proto/src/main/proto/ai/protomolt/proto/schema/registry/v1/descriptor_exchange.proto)
- [proto/ai/protomolt/proto/schema/registry/v1/registry_admin.proto](../../schema/registry/proto/src/main/proto/ai/protomolt/proto/schema/registry/v1/registry_admin.proto)

## schema/registry/service

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/registry/service/PublishConfigAction.java](../../schema/registry/service/src/main/java/ai/protomolt/proto/registry/service/PublishConfigAction.java)
- [java/ai/protomolt/proto/registry/service/SchemaRegistryServer.java](../../schema/registry/service/src/main/java/ai/protomolt/proto/registry/service/SchemaRegistryServer.java)
- [java/ai/protomolt/proto/registry/service/SchemaRegistryServerConfig.java](../../schema/registry/service/src/main/java/ai/protomolt/proto/registry/service/SchemaRegistryServerConfig.java)
- [java/ai/protomolt/proto/registry/service/package-info.java](../../schema/registry/service/src/main/java/ai/protomolt/proto/registry/service/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/registry/service/ActionsMountTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/ActionsMountTest.java)
- [java/ai/protomolt/proto/registry/service/ConfigCapabilityTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/ConfigCapabilityTest.java)
- [java/ai/protomolt/proto/registry/service/EmptyStore.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/EmptyStore.java)
- [java/ai/protomolt/proto/registry/service/InternalErrorSanitizationTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/InternalErrorSanitizationTest.java)
- [java/ai/protomolt/proto/registry/service/ParquetSchemaEndpointTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/ParquetSchemaEndpointTest.java)
- [java/ai/protomolt/proto/registry/service/PublishConfigActionTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/PublishConfigActionTest.java)
- [java/ai/protomolt/proto/registry/service/RegistryAdminServiceCoverageTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/RegistryAdminServiceCoverageTest.java)
- [java/ai/protomolt/proto/registry/service/RegistryRequestContractTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/RegistryRequestContractTest.java)
- [java/ai/protomolt/proto/registry/service/SchemaRegistryServerConfigTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/SchemaRegistryServerConfigTest.java)
- [java/ai/protomolt/proto/registry/service/SchemaRegistryServerConfigValidationTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/SchemaRegistryServerConfigValidationTest.java)
- [java/ai/protomolt/proto/registry/service/SchemaRegistryServerHttpTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/SchemaRegistryServerHttpTest.java)
- [java/ai/protomolt/proto/registry/service/SchemaRegistryServerRoundTripTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/SchemaRegistryServerRoundTripTest.java)
- [java/ai/protomolt/proto/registry/service/ScopedRegistryTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/ScopedRegistryTest.java)
- [java/ai/protomolt/proto/registry/service/TokenAuthTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/TokenAuthTest.java)
- [java/ai/protomolt/proto/registry/service/TopologicalOrderTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/TopologicalOrderTest.java)
- [java/ai/protomolt/proto/registry/service/WorkflowsEndpointTest.java](../../schema/registry/service/src/test/java/ai/protomolt/proto/registry/service/WorkflowsEndpointTest.java)

## search/index/lucene

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/search/index/lucene/LuceneFieldSpecs.java](../../search/index/lucene/src/main/java/ai/protomolt/proto/search/index/lucene/LuceneFieldSpecs.java)
- [java/ai/protomolt/proto/search/index/lucene/LuceneIndexWriter.java](../../search/index/lucene/src/main/java/ai/protomolt/proto/search/index/lucene/LuceneIndexWriter.java)
- [java/ai/protomolt/proto/search/index/lucene/LuceneIndexerProvider.java](../../search/index/lucene/src/main/java/ai/protomolt/proto/search/index/lucene/LuceneIndexerProvider.java)
- [java/ai/protomolt/proto/search/index/lucene/ProtoLuceneMapper.java](../../search/index/lucene/src/main/java/ai/protomolt/proto/search/index/lucene/ProtoLuceneMapper.java)
- [java/ai/protomolt/proto/search/index/lucene/package-info.java](../../search/index/lucene/src/main/java/ai/protomolt/proto/search/index/lucene/package-info.java)
- [resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider](../../search/index/lucene/src/main/resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/search/index/lucene/CanonicalRangeMappingTest.java](../../search/index/lucene/src/test/java/ai/protomolt/proto/search/index/lucene/CanonicalRangeMappingTest.java)
- [java/ai/protomolt/proto/search/index/lucene/LuceneEngineEndToEndTest.java](../../search/index/lucene/src/test/java/ai/protomolt/proto/search/index/lucene/LuceneEngineEndToEndTest.java)
- [java/ai/protomolt/proto/search/index/lucene/LuceneFieldSpecsTest.java](../../search/index/lucene/src/test/java/ai/protomolt/proto/search/index/lucene/LuceneFieldSpecsTest.java)
- [java/ai/protomolt/proto/search/index/lucene/LuceneIndexWriterTest.java](../../search/index/lucene/src/test/java/ai/protomolt/proto/search/index/lucene/LuceneIndexWriterTest.java)
- [java/ai/protomolt/proto/search/index/lucene/ProtoLuceneMapperFanOutTest.java](../../search/index/lucene/src/test/java/ai/protomolt/proto/search/index/lucene/ProtoLuceneMapperFanOutTest.java)
- [java/ai/protomolt/proto/search/index/lucene/ProtoLuceneMapperTest.java](../../search/index/lucene/src/test/java/ai/protomolt/proto/search/index/lucene/ProtoLuceneMapperTest.java)
- [java/ai/protomolt/proto/search/index/lucene/TreePathMappingTest.java](../../search/index/lucene/src/test/java/ai/protomolt/proto/search/index/lucene/TreePathMappingTest.java)

## search/index/ndjson

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/search/index/ndjson/NdjsonOptions.java](../../search/index/ndjson/src/main/java/ai/protomolt/proto/search/index/ndjson/NdjsonOptions.java)
- [java/ai/protomolt/proto/search/index/ndjson/ProtoNdjsonWriter.java](../../search/index/ndjson/src/main/java/ai/protomolt/proto/search/index/ndjson/ProtoNdjsonWriter.java)
- [java/ai/protomolt/proto/search/index/ndjson/package-info.java](../../search/index/ndjson/src/main/java/ai/protomolt/proto/search/index/ndjson/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/search/index/ndjson/ProtoNdjsonWriterTest.java](../../search/index/ndjson/src/test/java/ai/protomolt/proto/search/index/ndjson/ProtoNdjsonWriterTest.java)

## search/index/opensearch

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchDocumentMapper.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/OpenSearchDocumentMapper.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchHit.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/OpenSearchHit.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchIndexerProvider.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/OpenSearchIndexerProvider.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchMappingGenerator.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/OpenSearchMappingGenerator.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchSearch.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/OpenSearchSearch.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchSink.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/OpenSearchSink.java)
- [java/ai/protomolt/proto/search/index/opensearch/RankedHit.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/RankedHit.java)
- [java/ai/protomolt/proto/search/index/opensearch/RerankedSemanticSearch.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/RerankedSemanticSearch.java)
- [java/ai/protomolt/proto/search/index/opensearch/package-info.java](../../search/index/opensearch/src/main/java/ai/protomolt/proto/search/index/opensearch/package-info.java)
- [resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider](../../search/index/opensearch/src/main/resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/search/index/opensearch/CanonicalRangeMappingTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/CanonicalRangeMappingTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchDocumentMapperFanOutTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/OpenSearchDocumentMapperFanOutTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchDocumentMapperTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/OpenSearchDocumentMapperTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchIndexerProviderTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/OpenSearchIndexerProviderTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchLiveIntegrationTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/OpenSearchLiveIntegrationTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchMappingGeneratorTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/OpenSearchMappingGeneratorTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchSearchTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/OpenSearchSearchTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/OpenSearchSinkTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/OpenSearchSinkTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/RerankedSearchLiveIntegrationTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/RerankedSearchLiveIntegrationTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/RerankedSemanticSearchTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/RerankedSemanticSearchTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/TeiSemanticSearchLiveIntegrationTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/TeiSemanticSearchLiveIntegrationTest.java)
- [java/ai/protomolt/proto/search/index/opensearch/TreePathMappingTest.java](../../search/index/opensearch/src/test/java/ai/protomolt/proto/search/index/opensearch/TreePathMappingTest.java)

## search/index/protobuf

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/search/index/protobuf/DeclaredRulesAnyPayloadValidator.java](../../search/index/protobuf/src/main/java/ai/protomolt/proto/search/index/protobuf/DeclaredRulesAnyPayloadValidator.java)
- [java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexer.java](../../search/index/protobuf/src/main/java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexer.java)
- [java/ai/protomolt/proto/search/index/protobuf/package-info.java](../../search/index/protobuf/src/main/java/ai/protomolt/proto/search/index/protobuf/package-info.java)
- [resources/META-INF/services/ai.protomolt.proto.search.index.spi.AnyPayloadValidator](../../search/index/protobuf/src/main/resources/META-INF/services/ai.protomolt.proto.search.index.spi.AnyPayloadValidator)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/search/index/protobuf/AnyPayloadValidationTest.java](../../search/index/protobuf/src/test/java/ai/protomolt/proto/search/index/protobuf/AnyPayloadValidationTest.java)
- [java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexerAnyGateEdgeCaseTest.java](../../search/index/protobuf/src/test/java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexerAnyGateEdgeCaseTest.java)
- [java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexerAnyGateTest.java](../../search/index/protobuf/src/test/java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexerAnyGateTest.java)
- [java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexerTest.java](../../search/index/protobuf/src/test/java/ai/protomolt/proto/search/index/protobuf/ProtobufIndexerTest.java)
- [java/ai/protomolt/proto/search/index/protobuf/TaxonomyAwareAnyGateTest.java](../../search/index/protobuf/src/test/java/ai/protomolt/proto/search/index/protobuf/TaxonomyAwareAnyGateTest.java)
- [java/ai/protomolt/proto/search/index/protobuf/ValidatePayloadsOptOutFacadeTest.java](../../search/index/protobuf/src/test/java/ai/protomolt/proto/search/index/protobuf/ValidatePayloadsOptOutFacadeTest.java)
- [proto/ai/protomolt/proto/indexing/testdata/v1/indexable_doc.proto](../../search/index/protobuf/src/test/proto/ai/protomolt/proto/indexing/testdata/v1/indexable_doc.proto)

## search/index/qdrant

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/search/index/qdrant/QdrantConfig.java](../../search/index/qdrant/src/main/java/ai/protomolt/proto/search/index/qdrant/QdrantConfig.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantIndexerProvider.java](../../search/index/qdrant/src/main/java/ai/protomolt/proto/search/index/qdrant/QdrantIndexerProvider.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapper.java](../../search/index/qdrant/src/main/java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapper.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantSchemaGenerator.java](../../search/index/qdrant/src/main/java/ai/protomolt/proto/search/index/qdrant/QdrantSchemaGenerator.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantSink.java](../../search/index/qdrant/src/main/java/ai/protomolt/proto/search/index/qdrant/QdrantSink.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantVectorSpec.java](../../search/index/qdrant/src/main/java/ai/protomolt/proto/search/index/qdrant/QdrantVectorSpec.java)
- [java/ai/protomolt/proto/search/index/qdrant/package-info.java](../../search/index/qdrant/src/main/java/ai/protomolt/proto/search/index/qdrant/package-info.java)
- [proto/collections.proto](../../search/index/qdrant/src/main/proto/collections.proto)
- [proto/collections_service.proto](../../search/index/qdrant/src/main/proto/collections_service.proto)
- [proto/json_with_int.proto](../../search/index/qdrant/src/main/proto/json_with_int.proto)
- [proto/points.proto](../../search/index/qdrant/src/main/proto/points.proto)
- [proto/points_service.proto](../../search/index/qdrant/src/main/proto/points_service.proto)
- [proto/qdrant_common.proto](../../search/index/qdrant/src/main/proto/qdrant_common.proto)
- [resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider](../../search/index/qdrant/src/main/resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/search/index/qdrant/QdrantConfigTest.java](../../search/index/qdrant/src/test/java/ai/protomolt/proto/search/index/qdrant/QdrantConfigTest.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantIndexerProviderTest.java](../../search/index/qdrant/src/test/java/ai/protomolt/proto/search/index/qdrant/QdrantIndexerProviderTest.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantLiveIntegrationTest.java](../../search/index/qdrant/src/test/java/ai/protomolt/proto/search/index/qdrant/QdrantLiveIntegrationTest.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapperFanOutTest.java](../../search/index/qdrant/src/test/java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapperFanOutTest.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapperTest.java](../../search/index/qdrant/src/test/java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapperTest.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapperValidationTest.java](../../search/index/qdrant/src/test/java/ai/protomolt/proto/search/index/qdrant/QdrantPointMapperValidationTest.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantSchemaGeneratorTest.java](../../search/index/qdrant/src/test/java/ai/protomolt/proto/search/index/qdrant/QdrantSchemaGeneratorTest.java)
- [java/ai/protomolt/proto/search/index/qdrant/QdrantSinkTest.java](../../search/index/qdrant/src/test/java/ai/protomolt/proto/search/index/qdrant/QdrantSinkTest.java)

## search/index/solr

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/search/index/solr/SolrDocumentMapper.java](../../search/index/solr/src/main/java/ai/protomolt/proto/search/index/solr/SolrDocumentMapper.java)
- [java/ai/protomolt/proto/search/index/solr/SolrIndexerProvider.java](../../search/index/solr/src/main/java/ai/protomolt/proto/search/index/solr/SolrIndexerProvider.java)
- [java/ai/protomolt/proto/search/index/solr/SolrSchemaGenerator.java](../../search/index/solr/src/main/java/ai/protomolt/proto/search/index/solr/SolrSchemaGenerator.java)
- [java/ai/protomolt/proto/search/index/solr/package-info.java](../../search/index/solr/src/main/java/ai/protomolt/proto/search/index/solr/package-info.java)
- [resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider](../../search/index/solr/src/main/resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/search/index/solr/CanonicalRangeMappingTest.java](../../search/index/solr/src/test/java/ai/protomolt/proto/search/index/solr/CanonicalRangeMappingTest.java)
- [java/ai/protomolt/proto/search/index/solr/SolrDocumentMapperFanOutTest.java](../../search/index/solr/src/test/java/ai/protomolt/proto/search/index/solr/SolrDocumentMapperFanOutTest.java)
- [java/ai/protomolt/proto/search/index/solr/SolrDocumentMapperTest.java](../../search/index/solr/src/test/java/ai/protomolt/proto/search/index/solr/SolrDocumentMapperTest.java)
- [java/ai/protomolt/proto/search/index/solr/SolrLiveIntegrationTest.java](../../search/index/solr/src/test/java/ai/protomolt/proto/search/index/solr/SolrLiveIntegrationTest.java)
- [java/ai/protomolt/proto/search/index/solr/SolrSchemaGeneratorTest.java](../../search/index/solr/src/test/java/ai/protomolt/proto/search/index/solr/SolrSchemaGeneratorTest.java)
- [java/ai/protomolt/proto/search/index/solr/TreePathMappingTest.java](../../search/index/solr/src/test/java/ai/protomolt/proto/search/index/solr/TreePathMappingTest.java)

## search/index/spi

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/search/index/spi/AnyIndexing.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/AnyIndexing.java)
- [java/ai/protomolt/proto/search/index/spi/AnyPayloadGate.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/AnyPayloadGate.java)
- [java/ai/protomolt/proto/search/index/spi/AnyPayloadValidator.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/AnyPayloadValidator.java)
- [java/ai/protomolt/proto/search/index/spi/BlockRole.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/BlockRole.java)
- [java/ai/protomolt/proto/search/index/spi/CatalogIndexingHintSource.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/CatalogIndexingHintSource.java)
- [java/ai/protomolt/proto/search/index/spi/ChunkingPolicy.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/ChunkingPolicy.java)
- [java/ai/protomolt/proto/search/index/spi/DateResolution.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/DateResolution.java)
- [java/ai/protomolt/proto/search/index/spi/IndexFieldKind.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/IndexFieldKind.java)
- [java/ai/protomolt/proto/search/index/spi/IndexMapping.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/IndexMapping.java)
- [java/ai/protomolt/proto/search/index/spi/IndexMappingException.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/IndexMappingException.java)
- [java/ai/protomolt/proto/search/index/spi/IndexMappingFactory.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/IndexMappingFactory.java)
- [java/ai/protomolt/proto/search/index/spi/IndexerContext.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/IndexerContext.java)
- [java/ai/protomolt/proto/search/index/spi/IndexingHintSource.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/IndexingHintSource.java)
- [java/ai/protomolt/proto/search/index/spi/InferringIndexingHintSource.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/InferringIndexingHintSource.java)
- [java/ai/protomolt/proto/search/index/spi/MapMode.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/MapMode.java)
- [java/ai/protomolt/proto/search/index/spi/MappingValues.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/MappingValues.java)
- [java/ai/protomolt/proto/search/index/spi/ProtoOptionsIndexingHintSource.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/ProtoOptionsIndexingHintSource.java)
- [java/ai/protomolt/proto/search/index/spi/RangeBounds.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/RangeBounds.java)
- [java/ai/protomolt/proto/search/index/spi/ResolvedFieldHint.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/ResolvedFieldHint.java)
- [java/ai/protomolt/proto/search/index/spi/SearchEngineIndexer.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/SearchEngineIndexer.java)
- [java/ai/protomolt/proto/search/index/spi/SearchEngineIndexerProvider.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/SearchEngineIndexerProvider.java)
- [java/ai/protomolt/proto/search/index/spi/SearchEngineIndexers.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/SearchEngineIndexers.java)
- [java/ai/protomolt/proto/search/index/spi/TreePaths.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/TreePaths.java)
- [java/ai/protomolt/proto/search/index/spi/VectorElementType.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/VectorElementType.java)
- [java/ai/protomolt/proto/search/index/spi/VectorSimilarity.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/VectorSimilarity.java)
- [java/ai/protomolt/proto/search/index/spi/package-info.java](../../search/index/spi/src/main/java/ai/protomolt/proto/search/index/spi/package-info.java)
- [proto/ai/protomolt/proto/index/hints/v1/indexing_hints.proto](../../search/index/spi/src/main/proto/ai/protomolt/proto/index/hints/v1/indexing_hints.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/search/index/spi/AnyIndexingTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/AnyIndexingTest.java)
- [java/ai/protomolt/proto/search/index/spi/AnyPayloadGateEdgeCaseTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/AnyPayloadGateEdgeCaseTest.java)
- [java/ai/protomolt/proto/search/index/spi/AnyPayloadGateTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/AnyPayloadGateTest.java)
- [java/ai/protomolt/proto/search/index/spi/BlockVocabularyTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/BlockVocabularyTest.java)
- [java/ai/protomolt/proto/search/index/spi/CatalogIndexingHintSourceTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/CatalogIndexingHintSourceTest.java)
- [java/ai/protomolt/proto/search/index/spi/ChunkingPolicyTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/ChunkingPolicyTest.java)
- [java/ai/protomolt/proto/search/index/spi/IndexMappingExceptionTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/IndexMappingExceptionTest.java)
- [java/ai/protomolt/proto/search/index/spi/IndexMappingFactoryTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/IndexMappingFactoryTest.java)
- [java/ai/protomolt/proto/search/index/spi/IndexMappingTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/IndexMappingTest.java)
- [java/ai/protomolt/proto/search/index/spi/MappingSensitivityTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/MappingSensitivityTest.java)
- [java/ai/protomolt/proto/search/index/spi/MappingValuesEdgeCaseTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/MappingValuesEdgeCaseTest.java)
- [java/ai/protomolt/proto/search/index/spi/MappingValuesTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/MappingValuesTest.java)
- [java/ai/protomolt/proto/search/index/spi/RangeBoundsTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/RangeBoundsTest.java)
- [java/ai/protomolt/proto/search/index/spi/ResolvedFieldHintTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/ResolvedFieldHintTest.java)
- [java/ai/protomolt/proto/search/index/spi/SearchEngineIndexersTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/SearchEngineIndexersTest.java)
- [java/ai/protomolt/proto/search/index/spi/StubIndexerProvider.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/StubIndexerProvider.java)
- [java/ai/protomolt/proto/search/index/spi/TreePathsTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/TreePathsTest.java)
- [java/ai/protomolt/proto/search/index/spi/ValidatePayloadsOptOutTest.java](../../search/index/spi/src/test/java/ai/protomolt/proto/search/index/spi/ValidatePayloadsOptOutTest.java)
- [resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider](../../search/index/spi/src/test/resources/META-INF/services/ai.protomolt.proto.search.index.spi.SearchEngineIndexerProvider)

## sink/emit/core

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/emit/Bundle.java](../../sink/emit/core/src/main/java/ai/protomolt/proto/emit/Bundle.java)
- [java/ai/protomolt/proto/emit/BundleSink.java](../../sink/emit/core/src/main/java/ai/protomolt/proto/emit/BundleSink.java)
- [java/ai/protomolt/proto/emit/Bundles.java](../../sink/emit/core/src/main/java/ai/protomolt/proto/emit/Bundles.java)
- [java/ai/protomolt/proto/emit/DirectorySink.java](../../sink/emit/core/src/main/java/ai/protomolt/proto/emit/DirectorySink.java)
- [java/ai/protomolt/proto/emit/package-info.java](../../sink/emit/core/src/main/java/ai/protomolt/proto/emit/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/emit/EmitSinksTest.java](../../sink/emit/core/src/test/java/ai/protomolt/proto/emit/EmitSinksTest.java)

## sink/emit/git

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/emit/git/GitSink.java](../../sink/emit/git/src/main/java/ai/protomolt/proto/emit/git/GitSink.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/emit/GitSinkTest.java](../../sink/emit/git/src/test/java/ai/protomolt/proto/emit/GitSinkTest.java)

## sink/kafka/connect

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/kafka/connect/CelFilter.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/CelFilter.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcConnectorSupport.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcConnectorSupport.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSinkConfig.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcSinkConfig.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSinkConnector.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcSinkConnector.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSinkTask.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcSinkTask.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSourceConfig.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcSourceConfig.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSourceConnector.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcSourceConnector.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSourceTask.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/GrpcSourceTask.java)
- [java/ai/protomolt/proto/kafka/connect/MapMessage.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/MapMessage.java)
- [java/ai/protomolt/proto/kafka/connect/RedactMessage.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/RedactMessage.java)
- [java/ai/protomolt/proto/kafka/connect/ValidateMessage.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/ValidateMessage.java)
- [java/ai/protomolt/proto/kafka/connect/ValueCodec.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/ValueCodec.java)
- [java/ai/protomolt/proto/kafka/connect/package-info.java](../../sink/kafka/connect/src/main/java/ai/protomolt/proto/kafka/connect/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/kafka/connect/CelFilterTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/CelFilterTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcConnectorSupportResolveTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcConnectorSupportResolveTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcConnectorSupportTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcConnectorSupportTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSinkConfigTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcSinkConfigTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSinkTaskHeadersTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcSinkTaskHeadersTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSinkTaskTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcSinkTaskTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSourceConfigTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcSourceConfigTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSourceTaskPrepareTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcSourceTaskPrepareTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSourceTaskTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcSourceTaskTest.java)
- [java/ai/protomolt/proto/kafka/connect/GrpcSourceTaskTokenTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/GrpcSourceTaskTokenTest.java)
- [java/ai/protomolt/proto/kafka/connect/MapMessageTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/MapMessageTest.java)
- [java/ai/protomolt/proto/kafka/connect/RedactMessageTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/RedactMessageTest.java)
- [java/ai/protomolt/proto/kafka/connect/TransformsTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/TransformsTest.java)
- [java/ai/protomolt/proto/kafka/connect/ValidateMessageTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/ValidateMessageTest.java)
- [java/ai/protomolt/proto/kafka/connect/ValueCodecFormatsTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/ValueCodecFormatsTest.java)
- [java/ai/protomolt/proto/kafka/connect/ValueCodecTest.java](../../sink/kafka/connect/src/test/java/ai/protomolt/proto/kafka/connect/ValueCodecTest.java)

## sink/kafka/connect-iceberg

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/kafka/connect/iceberg/ConnectDescriptors.java](../../sink/kafka/connect-iceberg/src/main/java/ai/protomolt/proto/kafka/connect/iceberg/ConnectDescriptors.java)
- [java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkConfig.java](../../sink/kafka/connect-iceberg/src/main/java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkConfig.java)
- [java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkConnector.java](../../sink/kafka/connect-iceberg/src/main/java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkConnector.java)
- [java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkTask.java](../../sink/kafka/connect-iceberg/src/main/java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkTask.java)
- [java/ai/protomolt/proto/kafka/connect/iceberg/package-info.java](../../sink/kafka/connect-iceberg/src/main/java/ai/protomolt/proto/kafka/connect/iceberg/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/kafka/connect/iceberg/ConnectDescriptorsTest.java](../../sink/kafka/connect-iceberg/src/test/java/ai/protomolt/proto/kafka/connect/iceberg/ConnectDescriptorsTest.java)
- [java/ai/protomolt/proto/kafka/connect/iceberg/IcebergConnectLiveIntegrationTest.java](../../sink/kafka/connect-iceberg/src/test/java/ai/protomolt/proto/kafka/connect/iceberg/IcebergConnectLiveIntegrationTest.java)
- [java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkConfigTest.java](../../sink/kafka/connect-iceberg/src/test/java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkConfigTest.java)
- [java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkTaskTest.java](../../sink/kafka/connect-iceberg/src/test/java/ai/protomolt/proto/kafka/connect/iceberg/IcebergSinkTaskTest.java)

## sink/kafka/connect-opensearch

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/kafka/connect/opensearch/ConnectDescriptors.java](../../sink/kafka/connect-opensearch/src/main/java/ai/protomolt/proto/kafka/connect/opensearch/ConnectDescriptors.java)
- [java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConfig.java](../../sink/kafka/connect-opensearch/src/main/java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConfig.java)
- [java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConnector.java](../../sink/kafka/connect-opensearch/src/main/java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConnector.java)
- [java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkTask.java](../../sink/kafka/connect-opensearch/src/main/java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkTask.java)
- [java/ai/protomolt/proto/kafka/connect/opensearch/package-info.java](../../sink/kafka/connect-opensearch/src/main/java/ai/protomolt/proto/kafka/connect/opensearch/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConnectorEdgeCaseTest.java](../../sink/kafka/connect-opensearch/src/test/java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConnectorEdgeCaseTest.java)
- [java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConnectorTest.java](../../sink/kafka/connect-opensearch/src/test/java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkConnectorTest.java)
- [java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkTaskEdgeCaseTest.java](../../sink/kafka/connect-opensearch/src/test/java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkTaskEdgeCaseTest.java)
- [java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkTaskTest.java](../../sink/kafka/connect-opensearch/src/test/java/ai/protomolt/proto/kafka/connect/opensearch/OpenSearchSinkTaskTest.java)

## sink/kafka/serde

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/kafka/serde/GeneratedMessages.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/GeneratedMessages.java)
- [java/ai/protomolt/proto/kafka/serde/ProtoMoltProtobufDeserializer.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/ProtoMoltProtobufDeserializer.java)
- [java/ai/protomolt/proto/kafka/serde/ProtoMoltProtobufSerializer.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/ProtoMoltProtobufSerializer.java)
- [java/ai/protomolt/proto/kafka/serde/ProtoMoltSerde.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/ProtoMoltSerde.java)
- [java/ai/protomolt/proto/kafka/serde/ProtoMoltSerdeConfig.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/ProtoMoltSerdeConfig.java)
- [java/ai/protomolt/proto/kafka/serde/SchemaIds.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/SchemaIds.java)
- [java/ai/protomolt/proto/kafka/serde/SchemaLookup.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/SchemaLookup.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeDescriptors.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/SerdeDescriptors.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeMapper.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/SerdeMapper.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeMetricsListener.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/SerdeMetricsListener.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeMetricsListeners.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/SerdeMetricsListeners.java)
- [java/ai/protomolt/proto/kafka/serde/Subjects.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/Subjects.java)
- [java/ai/protomolt/proto/kafka/serde/package-info.java](../../sink/kafka/serde/src/main/java/ai/protomolt/proto/kafka/serde/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/kafka/serde/GeneratedClassSerdeTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/GeneratedClassSerdeTest.java)
- [java/ai/protomolt/proto/kafka/serde/GeneratedMessagesTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/GeneratedMessagesTest.java)
- [java/ai/protomolt/proto/kafka/serde/MultiTypeTopicTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/MultiTypeTopicTest.java)
- [java/ai/protomolt/proto/kafka/serde/ProtoMoltSerdeConfigTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/ProtoMoltSerdeConfigTest.java)
- [java/ai/protomolt/proto/kafka/serde/ProtoMoltSerdeTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/ProtoMoltSerdeTest.java)
- [java/ai/protomolt/proto/kafka/serde/RecordingMetricsListener.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/RecordingMetricsListener.java)
- [java/ai/protomolt/proto/kafka/serde/SchemaIdsTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SchemaIdsTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeConfigureEdgeCasesTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeConfigureEdgeCasesTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeConfluentInteropIntegrationTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeConfluentInteropIntegrationTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeDescriptorsTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeDescriptorsTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeFakeRegistryTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeFakeRegistryTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeLatestCompatibilityTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeLatestCompatibilityTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeMapperParseTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeMapperParseTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeMapperTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeMapperTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeMetricsListenersTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeMetricsListenersTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeMetricsTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeMetricsTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeQualityFloorTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeQualityFloorTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeQualityTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeQualityTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeRegistryFallbackTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeRegistryFallbackTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeRegistryLiveIntegrationTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeRegistryLiveIntegrationTest.java)
- [java/ai/protomolt/proto/kafka/serde/SerdeSubjectNamingTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SerdeSubjectNamingTest.java)
- [java/ai/protomolt/proto/kafka/serde/SubjectsTest.java](../../sink/kafka/serde/src/test/java/ai/protomolt/proto/kafka/serde/SubjectsTest.java)
- [proto/serde/gen/v1/gen_order.proto](../../sink/kafka/serde/src/test/proto/serde/gen/v1/gen_order.proto)
- [proto/serde/gen/v1/legacy_event.proto](../../sink/kafka/serde/src/test/proto/serde/gen/v1/legacy_event.proto)
- [resources/META-INF/services/ai.protomolt.proto.kafka.serde.SerdeMetricsListener](../../sink/kafka/serde/src/test/resources/META-INF/services/ai.protomolt.proto.kafka.serde.SerdeMetricsListener)

## sink/kafka/serde-micrometer

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/kafka/serde/micrometer/MicrometerSerdeMetrics.java](../../sink/kafka/serde-micrometer/src/main/java/ai/protomolt/proto/kafka/serde/micrometer/MicrometerSerdeMetrics.java)
- [java/ai/protomolt/proto/kafka/serde/micrometer/package-info.java](../../sink/kafka/serde-micrometer/src/main/java/ai/protomolt/proto/kafka/serde/micrometer/package-info.java)
- [resources/META-INF/services/ai.protomolt.proto.kafka.serde.SerdeMetricsListener](../../sink/kafka/serde-micrometer/src/main/resources/META-INF/services/ai.protomolt.proto.kafka.serde.SerdeMetricsListener)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/kafka/serde/micrometer/MicrometerSerdeMetricsTest.java](../../sink/kafka/serde-micrometer/src/test/java/ai/protomolt/proto/kafka/serde/micrometer/MicrometerSerdeMetricsTest.java)

## sink/kafka/wire

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/kafka/wire/ConfluentWireFormat.java](../../sink/kafka/wire/src/main/java/ai/protomolt/proto/kafka/wire/ConfluentWireFormat.java)
- [java/ai/protomolt/proto/kafka/wire/ConfluentWireFormatException.java](../../sink/kafka/wire/src/main/java/ai/protomolt/proto/kafka/wire/ConfluentWireFormatException.java)
- [java/ai/protomolt/proto/kafka/wire/package-info.java](../../sink/kafka/wire/src/main/java/ai/protomolt/proto/kafka/wire/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/kafka/wire/ConfluentWireFormatEdgeCasesTest.java](../../sink/kafka/wire/src/test/java/ai/protomolt/proto/kafka/wire/ConfluentWireFormatEdgeCasesTest.java)
- [java/ai/protomolt/proto/kafka/wire/ConfluentWireFormatTest.java](../../sink/kafka/wire/src/test/java/ai/protomolt/proto/kafka/wire/ConfluentWireFormatTest.java)

## surface/acp

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/acp/agent/CatalogLineRunner.java](../../surface/acp/src/main/java/ai/protomolt/proto/acp/agent/CatalogLineRunner.java)
- [java/ai/protomolt/proto/acp/agent/ProtoMoltAcpAgent.java](../../surface/acp/src/main/java/ai/protomolt/proto/acp/agent/ProtoMoltAcpAgent.java)
- [java/ai/protomolt/proto/acp/agent/RemoteCatalogLineRunner.java](../../surface/acp/src/main/java/ai/protomolt/proto/acp/agent/RemoteCatalogLineRunner.java)
- [java/ai/protomolt/proto/acp/agent/package-info.java](../../surface/acp/src/main/java/ai/protomolt/proto/acp/agent/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/acp/agent/AcpGrpcLive.java](../../surface/acp/src/test/java/ai/protomolt/proto/acp/agent/AcpGrpcLive.java)
- [java/ai/protomolt/proto/acp/agent/AcpSmoke.java](../../surface/acp/src/test/java/ai/protomolt/proto/acp/agent/AcpSmoke.java)
- [java/ai/protomolt/proto/acp/agent/CatalogLineRunnerTest.java](../../surface/acp/src/test/java/ai/protomolt/proto/acp/agent/CatalogLineRunnerTest.java)
- [java/ai/protomolt/proto/acp/agent/ProtoMoltAcpAgentProcessTest.java](../../surface/acp/src/test/java/ai/protomolt/proto/acp/agent/ProtoMoltAcpAgentProcessTest.java)
- [java/ai/protomolt/proto/acp/agent/ProtoMoltAcpAgentTest.java](../../surface/acp/src/test/java/ai/protomolt/proto/acp/agent/ProtoMoltAcpAgentTest.java)
- [java/ai/protomolt/proto/acp/agent/RemoteCatalogLineRunnerTest.java](../../surface/acp/src/test/java/ai/protomolt/proto/acp/agent/RemoteCatalogLineRunnerTest.java)
- [java/ai/protomolt/proto/acp/agent/RemoteDelegationLineRunnerTest.java](../../surface/acp/src/test/java/ai/protomolt/proto/acp/agent/RemoteDelegationLineRunnerTest.java)

## surface/actions

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/actions/ActionCatalog.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/ActionCatalog.java)
- [java/ai/protomolt/proto/actions/ActionContext.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/ActionContext.java)
- [java/ai/protomolt/proto/actions/ActionContractProvider.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/ActionContractProvider.java)
- [java/ai/protomolt/proto/actions/ActionException.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/ActionException.java)
- [java/ai/protomolt/proto/actions/ActionProvider.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/ActionProvider.java)
- [java/ai/protomolt/proto/actions/Caller.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/Caller.java)
- [java/ai/protomolt/proto/actions/CatalogContract.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/CatalogContract.java)
- [java/ai/protomolt/proto/actions/ContractStreamEmitter.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/ContractStreamEmitter.java)
- [java/ai/protomolt/proto/actions/EnvelopeTypes.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/EnvelopeTypes.java)
- [java/ai/protomolt/proto/actions/Fields.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/Fields.java)
- [java/ai/protomolt/proto/actions/Inputs.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/Inputs.java)
- [java/ai/protomolt/proto/actions/JsonStreamEmitter.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/JsonStreamEmitter.java)
- [java/ai/protomolt/proto/actions/ProtoAction.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/ProtoAction.java)
- [java/ai/protomolt/proto/actions/Reply.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/Reply.java)
- [java/ai/protomolt/proto/actions/SchemaResolver.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/SchemaResolver.java)
- [java/ai/protomolt/proto/actions/SchemaResolverProvider.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/SchemaResolverProvider.java)
- [java/ai/protomolt/proto/actions/ScopeBudgets.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/ScopeBudgets.java)
- [java/ai/protomolt/proto/actions/Scopes.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/Scopes.java)
- [java/ai/protomolt/proto/actions/StreamEmitter.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/StreamEmitter.java)
- [java/ai/protomolt/proto/actions/StreamingAction.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/StreamingAction.java)
- [java/ai/protomolt/proto/actions/package-info.java](../../surface/actions/src/main/java/ai/protomolt/proto/actions/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/actions/ActionCatalogCallerTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ActionCatalogCallerTest.java)
- [java/ai/protomolt/proto/actions/ActionCatalogScopeTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ActionCatalogScopeTest.java)
- [java/ai/protomolt/proto/actions/ActionCatalogTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ActionCatalogTest.java)
- [java/ai/protomolt/proto/actions/ActionContextTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ActionContextTest.java)
- [java/ai/protomolt/proto/actions/ActionExceptionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ActionExceptionTest.java)
- [java/ai/protomolt/proto/actions/ActionRequestContractTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ActionRequestContractTest.java)
- [java/ai/protomolt/proto/actions/ActionResponseContractTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ActionResponseContractTest.java)
- [java/ai/protomolt/proto/actions/CallerTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/CallerTest.java)
- [java/ai/protomolt/proto/actions/CatalogContractCauseTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/CatalogContractCauseTest.java)
- [java/ai/protomolt/proto/actions/CatalogIndexTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/CatalogIndexTest.java)
- [java/ai/protomolt/proto/actions/CatalogLedgerBoundaryTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/CatalogLedgerBoundaryTest.java)
- [java/ai/protomolt/proto/actions/CheckCompatActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/CheckCompatActionTest.java)
- [java/ai/protomolt/proto/actions/CheckRulesActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/CheckRulesActionTest.java)
- [java/ai/protomolt/proto/actions/CompileActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/CompileActionTest.java)
- [java/ai/protomolt/proto/actions/DiffSchemasActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/DiffSchemasActionTest.java)
- [java/ai/protomolt/proto/actions/EvalCelActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/EvalCelActionTest.java)
- [java/ai/protomolt/proto/actions/ExtractMetadataActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ExtractMetadataActionTest.java)
- [java/ai/protomolt/proto/actions/InferSchemaActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/InferSchemaActionTest.java)
- [java/ai/protomolt/proto/actions/InputsTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/InputsTest.java)
- [java/ai/protomolt/proto/actions/ListTypesActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ListTypesActionTest.java)
- [java/ai/protomolt/proto/actions/LlmLoopTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/LlmLoopTest.java)
- [java/ai/protomolt/proto/actions/MapMessageActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/MapMessageActionTest.java)
- [java/ai/protomolt/proto/actions/MaskMessageActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/MaskMessageActionTest.java)
- [java/ai/protomolt/proto/actions/MergeSchemasActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/MergeSchemasActionTest.java)
- [java/ai/protomolt/proto/actions/OptionalActionsTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/OptionalActionsTest.java)
- [java/ai/protomolt/proto/actions/RenderIndexMappingsActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/RenderIndexMappingsActionTest.java)
- [java/ai/protomolt/proto/actions/RenderJsonSchemaActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/RenderJsonSchemaActionTest.java)
- [java/ai/protomolt/proto/actions/RenderPromptActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/RenderPromptActionTest.java)
- [java/ai/protomolt/proto/actions/SchemaProvidersTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/SchemaProvidersTest.java)
- [java/ai/protomolt/proto/actions/SchemaSourceConventionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/SchemaSourceConventionTest.java)
- [java/ai/protomolt/proto/actions/ScopeBudgetsTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ScopeBudgetsTest.java)
- [java/ai/protomolt/proto/actions/ShapeActionsTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ShapeActionsTest.java)
- [java/ai/protomolt/proto/actions/StreamDeliveryFailureTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/StreamDeliveryFailureTest.java)
- [java/ai/protomolt/proto/actions/TestFixtures.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/TestFixtures.java)
- [java/ai/protomolt/proto/actions/ValidateMessageActionTest.java](../../surface/actions/src/test/java/ai/protomolt/proto/actions/ValidateMessageActionTest.java)

## surface/actions/index

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/actions/index/IndexActions.java](../../surface/actions/index/src/main/java/ai/protomolt/proto/actions/index/IndexActions.java)
- [java/ai/protomolt/proto/actions/index/RenderIndexMappingsAction.java](../../surface/actions/index/src/main/java/ai/protomolt/proto/actions/index/RenderIndexMappingsAction.java)
- [resources/META-INF/services/ai.protomolt.proto.actions.ActionProvider](../../surface/actions/index/src/main/resources/META-INF/services/ai.protomolt.proto.actions.ActionProvider)

## surface/actions/schema

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/actions/schema/ToolkitContracts.java](../../surface/actions/schema/src/main/java/ai/protomolt/proto/actions/schema/ToolkitContracts.java)
- [java/ai/protomolt/proto/actions/schema/ToolkitSchemaResolver.java](../../surface/actions/schema/src/main/java/ai/protomolt/proto/actions/schema/ToolkitSchemaResolver.java)
- [resources/META-INF/services/ai.protomolt.proto.actions.ActionContractProvider](../../surface/actions/schema/src/main/resources/META-INF/services/ai.protomolt.proto.actions.ActionContractProvider)
- [resources/META-INF/services/ai.protomolt.proto.actions.SchemaResolverProvider](../../surface/actions/schema/src/main/resources/META-INF/services/ai.protomolt.proto.actions.SchemaResolverProvider)

## surface/actions/toolkit

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/actions/toolkit/ActionJson.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/ActionJson.java)
- [java/ai/protomolt/proto/actions/toolkit/CheckCompatAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/CheckCompatAction.java)
- [java/ai/protomolt/proto/actions/toolkit/CheckRulesAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/CheckRulesAction.java)
- [java/ai/protomolt/proto/actions/toolkit/CompileAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/CompileAction.java)
- [java/ai/protomolt/proto/actions/toolkit/DiffSchemasAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/DiffSchemasAction.java)
- [java/ai/protomolt/proto/actions/toolkit/EvalCelAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/EvalCelAction.java)
- [java/ai/protomolt/proto/actions/toolkit/ExtractMetadataAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/ExtractMetadataAction.java)
- [java/ai/protomolt/proto/actions/toolkit/InferSchemaAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/InferSchemaAction.java)
- [java/ai/protomolt/proto/actions/toolkit/JoinMessagesAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/JoinMessagesAction.java)
- [java/ai/protomolt/proto/actions/toolkit/ListTypesAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/ListTypesAction.java)
- [java/ai/protomolt/proto/actions/toolkit/MapMessageAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/MapMessageAction.java)
- [java/ai/protomolt/proto/actions/toolkit/MaskMessageAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/MaskMessageAction.java)
- [java/ai/protomolt/proto/actions/toolkit/MergeSchemasAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/MergeSchemasAction.java)
- [java/ai/protomolt/proto/actions/toolkit/RenderJsonSchemaAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/RenderJsonSchemaAction.java)
- [java/ai/protomolt/proto/actions/toolkit/RenderPromptAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/RenderPromptAction.java)
- [java/ai/protomolt/proto/actions/toolkit/SynthesizeShapeAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/SynthesizeShapeAction.java)
- [java/ai/protomolt/proto/actions/toolkit/ToolkitActions.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/ToolkitActions.java)
- [java/ai/protomolt/proto/actions/toolkit/ValidateMessageAction.java](../../surface/actions/toolkit/src/main/java/ai/protomolt/proto/actions/toolkit/ValidateMessageAction.java)
- [resources/META-INF/services/ai.protomolt.proto.actions.ActionProvider](../../surface/actions/toolkit/src/main/resources/META-INF/services/ai.protomolt.proto.actions.ActionProvider)

## surface/grpc/adapter

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/adapter/CatalogBridge.java](../../surface/grpc/adapter/src/main/java/ai/protomolt/proto/grpc/adapter/CatalogBridge.java)
- [java/ai/protomolt/proto/grpc/adapter/ContractActionBindings.java](../../surface/grpc/adapter/src/main/java/ai/protomolt/proto/grpc/adapter/ContractActionBindings.java)
- [java/ai/protomolt/proto/grpc/adapter/GrpcActionService.java](../../surface/grpc/adapter/src/main/java/ai/protomolt/proto/grpc/adapter/GrpcActionService.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/adapter/GrpcActionServiceTest.java](../../surface/grpc/adapter/src/test/java/ai/protomolt/proto/grpc/adapter/GrpcActionServiceTest.java)

## surface/grpc/channel-policy

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/policy/OutboundChannelPolicy.java](../../surface/grpc/channel-policy/src/main/java/ai/protomolt/proto/grpc/policy/OutboundChannelPolicy.java)
- [java/ai/protomolt/proto/grpc/policy/OutboundChannelPolicyException.java](../../surface/grpc/channel-policy/src/main/java/ai/protomolt/proto/grpc/policy/OutboundChannelPolicyException.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/policy/OutboundChannelPolicyTest.java](../../surface/grpc/channel-policy/src/test/java/ai/protomolt/proto/grpc/policy/OutboundChannelPolicyTest.java)

## surface/grpc/invoke

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/invoke/BidiStreamingSink.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/BidiStreamingSink.java)
- [java/ai/protomolt/proto/grpc/invoke/ChannelFactory.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/ChannelFactory.java)
- [java/ai/protomolt/proto/grpc/invoke/ClientStreamingSink.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/ClientStreamingSink.java)
- [java/ai/protomolt/proto/grpc/invoke/DynamicGrpcCalls.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/DynamicGrpcCalls.java)
- [java/ai/protomolt/proto/grpc/invoke/DynamicGrpcStream.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/DynamicGrpcStream.java)
- [java/ai/protomolt/proto/grpc/invoke/GrpcInvokeAction.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/GrpcInvokeAction.java)
- [java/ai/protomolt/proto/grpc/invoke/ReflectAction.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/ReflectAction.java)
- [java/ai/protomolt/proto/grpc/invoke/ReflectionClient.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/ReflectionClient.java)
- [java/ai/protomolt/proto/grpc/invoke/ReflectionException.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/ReflectionException.java)
- [java/ai/protomolt/proto/grpc/invoke/package-info.java](../../surface/grpc/invoke/src/main/java/ai/protomolt/proto/grpc/invoke/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/invoke/ChannelFactoryPolicyTest.java](../../surface/grpc/invoke/src/test/java/ai/protomolt/proto/grpc/invoke/ChannelFactoryPolicyTest.java)
- [java/ai/protomolt/proto/grpc/invoke/ChannelFactoryTlsTest.java](../../surface/grpc/invoke/src/test/java/ai/protomolt/proto/grpc/invoke/ChannelFactoryTlsTest.java)
- [java/ai/protomolt/proto/grpc/invoke/DynamicGrpcStreamNettyTest.java](../../surface/grpc/invoke/src/test/java/ai/protomolt/proto/grpc/invoke/DynamicGrpcStreamNettyTest.java)
- [java/ai/protomolt/proto/grpc/invoke/DynamicGrpcStreamTest.java](../../surface/grpc/invoke/src/test/java/ai/protomolt/proto/grpc/invoke/DynamicGrpcStreamTest.java)
- [java/ai/protomolt/proto/grpc/invoke/GrpcInvokeActionTest.java](../../surface/grpc/invoke/src/test/java/ai/protomolt/proto/grpc/invoke/GrpcInvokeActionTest.java)
- [java/ai/protomolt/proto/grpc/invoke/ReflectActionTest.java](../../surface/grpc/invoke/src/test/java/ai/protomolt/proto/grpc/invoke/ReflectActionTest.java)
- [java/ai/protomolt/proto/grpc/invoke/ReflectionClientTest.java](../../surface/grpc/invoke/src/test/java/ai/protomolt/proto/grpc/invoke/ReflectionClientTest.java)

## surface/grpc/invoke/service

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/invoke/service/GrpcInvokeModule.java](../../surface/grpc/invoke/service/src/main/java/ai/protomolt/proto/grpc/invoke/service/GrpcInvokeModule.java)
- [resources/META-INF/services/ai.protomolt.proto.composer.ServiceModule](../../surface/grpc/invoke/service/src/main/resources/META-INF/services/ai.protomolt.proto.composer.ServiceModule)

## surface/grpc/service

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/service/CatalogBridge.java](../../surface/grpc/service/src/main/java/ai/protomolt/proto/grpc/service/CatalogBridge.java)
- [java/ai/protomolt/proto/grpc/service/ContractActionBindings.java](../../surface/grpc/service/src/main/java/ai/protomolt/proto/grpc/service/ContractActionBindings.java)
- [java/ai/protomolt/proto/grpc/service/ProtoMoltCatalog.java](../../surface/grpc/service/src/main/java/ai/protomolt/proto/grpc/service/ProtoMoltCatalog.java)
- [java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcServer.java](../../surface/grpc/service/src/main/java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcServer.java)
- [java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcService.java](../../surface/grpc/service/src/main/java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcService.java)
- [java/ai/protomolt/proto/grpc/service/package-info.java](../../surface/grpc/service/src/main/java/ai/protomolt/proto/grpc/service/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/service/ApiTokenServerInterceptorTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/ApiTokenServerInterceptorTest.java)
- [java/ai/protomolt/proto/grpc/service/ContractActionBindingsTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/ContractActionBindingsTest.java)
- [java/ai/protomolt/proto/grpc/service/ErrorContractTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/ErrorContractTest.java)
- [java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcServerTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcServerTest.java)
- [java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcServiceTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/ProtoMoltGrpcServiceTest.java)
- [java/ai/protomolt/proto/grpc/service/ScopedGrpcServiceTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/ScopedGrpcServiceTest.java)
- [java/ai/protomolt/proto/grpc/service/StructuredWorkflowCatalogTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/StructuredWorkflowCatalogTest.java)
- [java/ai/protomolt/proto/grpc/service/SurfaceParityTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/SurfaceParityTest.java)
- [java/ai/protomolt/proto/grpc/service/VerbRpcCorrespondenceTest.java](../../surface/grpc/service/src/test/java/ai/protomolt/proto/grpc/service/VerbRpcCorrespondenceTest.java)

## surface/grpc/service-contract

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/service/contract/ProtoMoltServiceSchema.java](../../surface/grpc/service-contract/src/main/java/ai/protomolt/proto/grpc/service/contract/ProtoMoltServiceSchema.java)
- [resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto](../../surface/grpc/service-contract/src/main/resources/ai/protomolt/proto/grpc/service/v1/protomolt_service.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/service/contract/CoreVerbContractTest.java](../../surface/grpc/service-contract/src/test/java/ai/protomolt/proto/grpc/service/contract/CoreVerbContractTest.java)
- [java/ai/protomolt/proto/grpc/service/contract/ProtoMoltServiceSchemaTest.java](../../surface/grpc/service-contract/src/test/java/ai/protomolt/proto/grpc/service/contract/ProtoMoltServiceSchemaTest.java)
- [java/ai/protomolt/proto/grpc/service/contract/ServiceWorkspaceContractTest.java](../../surface/grpc/service-contract/src/test/java/ai/protomolt/proto/grpc/service/contract/ServiceWorkspaceContractTest.java)
- [java/ai/protomolt/proto/grpc/service/contract/WorkflowStageContractTest.java](../../surface/grpc/service-contract/src/test/java/ai/protomolt/proto/grpc/service/contract/WorkflowStageContractTest.java)

## surface/grpc/service-profile

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/profile/FileSystemServiceProfileRepository.java](../../surface/grpc/service-profile/src/main/java/ai/protomolt/proto/grpc/profile/FileSystemServiceProfileRepository.java)
- [java/ai/protomolt/proto/grpc/profile/ServiceProfileRepository.java](../../surface/grpc/service-profile/src/main/java/ai/protomolt/proto/grpc/profile/ServiceProfileRepository.java)
- [java/ai/protomolt/proto/grpc/profile/ServiceProfileValidation.java](../../surface/grpc/service-profile/src/main/java/ai/protomolt/proto/grpc/profile/ServiceProfileValidation.java)
- [proto/ai/protomolt/proto/grpc/profile/v1/service_profile.proto](../../surface/grpc/service-profile/src/main/proto/ai/protomolt/proto/grpc/profile/v1/service_profile.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/profile/FileSystemServiceProfileRepositoryTest.java](../../surface/grpc/service-profile/src/test/java/ai/protomolt/proto/grpc/profile/FileSystemServiceProfileRepositoryTest.java)
- [java/ai/protomolt/proto/grpc/profile/ServiceProfileContractTest.java](../../surface/grpc/service-profile/src/test/java/ai/protomolt/proto/grpc/profile/ServiceProfileContractTest.java)
- [java/ai/protomolt/proto/grpc/profile/ServiceProfileValidationTest.java](../../surface/grpc/service-profile/src/test/java/ai/protomolt/proto/grpc/profile/ServiceProfileValidationTest.java)
- [java/ai/protomolt/proto/grpc/profile/TestProfiles.java](../../surface/grpc/service-profile/src/test/java/ai/protomolt/proto/grpc/profile/TestProfiles.java)

## surface/grpc/service-workspace

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/workspace/EnvProfileCredentialResolver.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/EnvProfileCredentialResolver.java)
- [java/ai/protomolt/proto/grpc/workspace/ProfileCredentialResolver.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ProfileCredentialResolver.java)
- [java/ai/protomolt/proto/grpc/workspace/ProfileCredentials.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ProfileCredentials.java)
- [java/ai/protomolt/proto/grpc/workspace/ProfileMethodPolicy.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ProfileMethodPolicy.java)
- [java/ai/protomolt/proto/grpc/workspace/ProfileStored.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ProfileStored.java)
- [java/ai/protomolt/proto/grpc/workspace/ReflectedMethodAction.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ReflectedMethodAction.java)
- [java/ai/protomolt/proto/grpc/workspace/ReflectedServiceActions.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ReflectedServiceActions.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceActionJson.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceActionJson.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceActionSupport.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceActionSupport.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceDescriptorInspection.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceDescriptorInspection.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceInspectAction.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceInspectAction.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceInvokeAction.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceInvokeAction.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceListAction.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceListAction.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceRefreshAction.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceRefreshAction.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceRegisterAction.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceRegisterAction.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceWorkspaceActions.java](../../surface/grpc/service-workspace/src/main/java/ai/protomolt/proto/grpc/workspace/ServiceWorkspaceActions.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/workspace/GitBackedReflectedAnyActionTest.java](../../surface/grpc/service-workspace/src/test/java/ai/protomolt/proto/grpc/workspace/GitBackedReflectedAnyActionTest.java)
- [java/ai/protomolt/proto/grpc/workspace/ReflectedAnyActionTest.java](../../surface/grpc/service-workspace/src/test/java/ai/protomolt/proto/grpc/workspace/ReflectedAnyActionTest.java)
- [java/ai/protomolt/proto/grpc/workspace/ReflectedVerbNamingTest.java](../../surface/grpc/service-workspace/src/test/java/ai/protomolt/proto/grpc/workspace/ReflectedVerbNamingTest.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceInspectContractTest.java](../../surface/grpc/service-workspace/src/test/java/ai/protomolt/proto/grpc/workspace/ServiceInspectContractTest.java)
- [java/ai/protomolt/proto/grpc/workspace/ServiceWorkspaceActionsTest.java](../../surface/grpc/service-workspace/src/test/java/ai/protomolt/proto/grpc/workspace/ServiceWorkspaceActionsTest.java)

## surface/grpc/validation

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/validate/GrpcValidationMetricsListener.java](../../surface/grpc/validation/src/main/java/ai/protomolt/proto/grpc/validate/GrpcValidationMetricsListener.java)
- [java/ai/protomolt/proto/grpc/validate/GrpcValidationMetricsListeners.java](../../surface/grpc/validation/src/main/java/ai/protomolt/proto/grpc/validate/GrpcValidationMetricsListeners.java)
- [java/ai/protomolt/proto/grpc/validate/ValidatingClientInterceptor.java](../../surface/grpc/validation/src/main/java/ai/protomolt/proto/grpc/validate/ValidatingClientInterceptor.java)
- [java/ai/protomolt/proto/grpc/validate/ValidatingServerInterceptor.java](../../surface/grpc/validation/src/main/java/ai/protomolt/proto/grpc/validate/ValidatingServerInterceptor.java)
- [java/ai/protomolt/proto/grpc/validate/package-info.java](../../surface/grpc/validation/src/main/java/ai/protomolt/proto/grpc/validate/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/validate/GrpcValidationMetricsListenersTest.java](../../surface/grpc/validation/src/test/java/ai/protomolt/proto/grpc/validate/GrpcValidationMetricsListenersTest.java)
- [java/ai/protomolt/proto/grpc/validate/ValidatingInterceptorTest.java](../../surface/grpc/validation/src/test/java/ai/protomolt/proto/grpc/validate/ValidatingInterceptorTest.java)

## surface/grpc/validation-micrometer

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/validate/micrometer/MicrometerGrpcValidationMetrics.java](../../surface/grpc/validation-micrometer/src/main/java/ai/protomolt/proto/grpc/validate/micrometer/MicrometerGrpcValidationMetrics.java)
- [java/ai/protomolt/proto/grpc/validate/micrometer/package-info.java](../../surface/grpc/validation-micrometer/src/main/java/ai/protomolt/proto/grpc/validate/micrometer/package-info.java)
- [resources/META-INF/services/ai.protomolt.proto.grpc.validate.GrpcValidationMetricsListener](../../surface/grpc/validation-micrometer/src/main/resources/META-INF/services/ai.protomolt.proto.grpc.validate.GrpcValidationMetricsListener)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/validate/micrometer/MicrometerGrpcValidationMetricsTest.java](../../surface/grpc/validation-micrometer/src/test/java/ai/protomolt/proto/grpc/validate/micrometer/MicrometerGrpcValidationMetricsTest.java)

## surface/grpc/workflow

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/grpc/workflow/ArtifactRepository.java](../../surface/grpc/workflow/src/main/java/ai/protomolt/proto/grpc/workflow/ArtifactRepository.java)
- [java/ai/protomolt/proto/grpc/workflow/FileSystemArtifactRepository.java](../../surface/grpc/workflow/src/main/java/ai/protomolt/proto/grpc/workflow/FileSystemArtifactRepository.java)
- [java/ai/protomolt/proto/grpc/workflow/FileSystemRunEvidenceRepository.java](../../surface/grpc/workflow/src/main/java/ai/protomolt/proto/grpc/workflow/FileSystemRunEvidenceRepository.java)
- [java/ai/protomolt/proto/grpc/workflow/RunEvidenceRepository.java](../../surface/grpc/workflow/src/main/java/ai/protomolt/proto/grpc/workflow/RunEvidenceRepository.java)
- [java/ai/protomolt/proto/grpc/workflow/WorkflowValidation.java](../../surface/grpc/workflow/src/main/java/ai/protomolt/proto/grpc/workflow/WorkflowValidation.java)
- [java/ai/protomolt/proto/grpc/workflow/WorkflowVersionRepository.java](../../surface/grpc/workflow/src/main/java/ai/protomolt/proto/grpc/workflow/WorkflowVersionRepository.java)
- [proto/ai/protomolt/proto/grpc/workflow/v1/grpc_workflow.proto](../../surface/grpc/workflow/src/main/proto/ai/protomolt/proto/grpc/workflow/v1/grpc_workflow.proto)
- [proto/ai/protomolt/proto/grpc/workflow/v1/workflow_assessment.proto](../../surface/grpc/workflow/src/main/proto/ai/protomolt/proto/grpc/workflow/v1/workflow_assessment.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/grpc/workflow/ArtifactRepositoryContractTest.java](../../surface/grpc/workflow/src/test/java/ai/protomolt/proto/grpc/workflow/ArtifactRepositoryContractTest.java)
- [java/ai/protomolt/proto/grpc/workflow/EdgeWorkflowValidationTest.java](../../surface/grpc/workflow/src/test/java/ai/protomolt/proto/grpc/workflow/EdgeWorkflowValidationTest.java)
- [java/ai/protomolt/proto/grpc/workflow/FileSystemArtifactRepositoryTest.java](../../surface/grpc/workflow/src/test/java/ai/protomolt/proto/grpc/workflow/FileSystemArtifactRepositoryTest.java)
- [java/ai/protomolt/proto/grpc/workflow/FileSystemRunEvidenceRepositoryTest.java](../../surface/grpc/workflow/src/test/java/ai/protomolt/proto/grpc/workflow/FileSystemRunEvidenceRepositoryTest.java)
- [java/ai/protomolt/proto/grpc/workflow/StructuredWorkflowValidationTest.java](../../surface/grpc/workflow/src/test/java/ai/protomolt/proto/grpc/workflow/StructuredWorkflowValidationTest.java)
- [java/ai/protomolt/proto/grpc/workflow/TestWorkflows.java](../../surface/grpc/workflow/src/test/java/ai/protomolt/proto/grpc/workflow/TestWorkflows.java)
- [java/ai/protomolt/proto/grpc/workflow/WorkflowContractTest.java](../../surface/grpc/workflow/src/test/java/ai/protomolt/proto/grpc/workflow/WorkflowContractTest.java)
- [java/ai/protomolt/proto/grpc/workflow/WorkflowValidationTest.java](../../surface/grpc/workflow/src/test/java/ai/protomolt/proto/grpc/workflow/WorkflowValidationTest.java)

## surface/http/json

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/http/json/MalformedProtobufJsonException.java](../../surface/http/json/src/main/java/ai/protomolt/proto/http/json/MalformedProtobufJsonException.java)
- [java/ai/protomolt/proto/http/json/ProtobufJsonException.java](../../surface/http/json/src/main/java/ai/protomolt/proto/http/json/ProtobufJsonException.java)
- [java/ai/protomolt/proto/http/json/ProtobufJsonTranscoder.java](../../surface/http/json/src/main/java/ai/protomolt/proto/http/json/ProtobufJsonTranscoder.java)
- [java/ai/protomolt/proto/http/json/package-info.java](../../surface/http/json/src/main/java/ai/protomolt/proto/http/json/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/http/json/ProtobufJsonTranscoderTest.java](../../surface/http/json/src/test/java/ai/protomolt/proto/http/json/ProtobufJsonTranscoderTest.java)

## surface/http/jsonschema

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/http/jsonschema/DecimalRangePattern.java](../../surface/http/jsonschema/src/main/java/ai/protomolt/proto/http/jsonschema/DecimalRangePattern.java)
- [java/ai/protomolt/proto/http/jsonschema/ProtoJsonSchemaGenerator.java](../../surface/http/jsonschema/src/main/java/ai/protomolt/proto/http/jsonschema/ProtoJsonSchemaGenerator.java)
- [java/ai/protomolt/proto/http/jsonschema/package-info.java](../../surface/http/jsonschema/src/main/java/ai/protomolt/proto/http/jsonschema/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/http/jsonschema/DecimalRangePatternTest.java](../../surface/http/jsonschema/src/test/java/ai/protomolt/proto/http/jsonschema/DecimalRangePatternTest.java)
- [java/ai/protomolt/proto/http/jsonschema/ProtoJsonSchemaGeneratorTest.java](../../surface/http/jsonschema/src/test/java/ai/protomolt/proto/http/jsonschema/ProtoJsonSchemaGeneratorTest.java)
- [proto/ai/protomolt/proto/http/jsonschema/testdata/v1/account.proto](../../surface/http/jsonschema/src/test/proto/ai/protomolt/proto/http/jsonschema/testdata/v1/account.proto)

## surface/http/openapi

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/http/openapi/OpenApiValidation.java](../../surface/http/openapi/src/main/java/ai/protomolt/proto/http/openapi/OpenApiValidation.java)
- [java/ai/protomolt/proto/http/openapi/ProtoOpenApiGenerator.java](../../surface/http/openapi/src/main/java/ai/protomolt/proto/http/openapi/ProtoOpenApiGenerator.java)
- [java/ai/protomolt/proto/http/openapi/package-info.java](../../surface/http/openapi/src/main/java/ai/protomolt/proto/http/openapi/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/http/openapi/OpenApiRuntimeGapTest.java](../../surface/http/openapi/src/test/java/ai/protomolt/proto/http/openapi/OpenApiRuntimeGapTest.java)
- [java/ai/protomolt/proto/http/openapi/ProtoOpenApiGeneratorTest.java](../../surface/http/openapi/src/test/java/ai/protomolt/proto/http/openapi/ProtoOpenApiGeneratorTest.java)
- [java/ai/protomolt/proto/http/openapi/ProtoOpenApiValidationParityTest.java](../../surface/http/openapi/src/test/java/ai/protomolt/proto/http/openapi/ProtoOpenApiValidationParityTest.java)
- [proto/ai/protomolt/proto/http/openapi/testdata/v1/contract.proto](../../surface/http/openapi/src/test/proto/ai/protomolt/proto/http/openapi/testdata/v1/contract.proto)
- [proto/ai/protomolt/proto/http/openapi/testdata/v1/runtime_gaps.proto](../../surface/http/openapi/src/test/proto/ai/protomolt/proto/http/openapi/testdata/v1/runtime_gaps.proto)

## surface/http/rest

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/http/rest/ApiTokenRequirement.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ApiTokenRequirement.java)
- [java/ai/protomolt/proto/http/rest/ForbiddenProtoRestException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ForbiddenProtoRestException.java)
- [java/ai/protomolt/proto/http/rest/HttpMethodNotAllowedException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/HttpMethodNotAllowedException.java)
- [java/ai/protomolt/proto/http/rest/MalformedRequestException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/MalformedRequestException.java)
- [java/ai/protomolt/proto/http/rest/MethodNotFoundException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/MethodNotFoundException.java)
- [java/ai/protomolt/proto/http/rest/ProtoApiToken.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoApiToken.java)
- [java/ai/protomolt/proto/http/rest/ProtoApiTokenValidator.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoApiTokenValidator.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestAnnotationRegistrar.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestAnnotationRegistrar.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestContextInvoker.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestContextInvoker.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestException.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestExposed.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestExposed.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestGateway.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestGateway.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestInvocationException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestInvocationException.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestMethod.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestMethod.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestMethodRegistry.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ProtoRestMethodRegistry.java)
- [java/ai/protomolt/proto/http/rest/RequestTooLargeException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/RequestTooLargeException.java)
- [java/ai/protomolt/proto/http/rest/ServiceNotFoundException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/ServiceNotFoundException.java)
- [java/ai/protomolt/proto/http/rest/UnauthorizedProtoRestException.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/UnauthorizedProtoRestException.java)
- [java/ai/protomolt/proto/http/rest/package-info.java](../../surface/http/rest/src/main/java/ai/protomolt/proto/http/rest/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/http/rest/ApiTokenRequirementTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ApiTokenRequirementTest.java)
- [java/ai/protomolt/proto/http/rest/ContextInvokerGatewayTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ContextInvokerGatewayTest.java)
- [java/ai/protomolt/proto/http/rest/ProtoApiTokenValidatorTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ProtoApiTokenValidatorTest.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestAnnotationRegistrarTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ProtoRestAnnotationRegistrarTest.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestExceptionsTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ProtoRestExceptionsTest.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestGatewayDispatchTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ProtoRestGatewayDispatchTest.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestGatewayTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ProtoRestGatewayTest.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestMethodRegistryTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ProtoRestMethodRegistryTest.java)
- [java/ai/protomolt/proto/http/rest/ProtoRestMethodTest.java](../../surface/http/rest/src/test/java/ai/protomolt/proto/http/rest/ProtoRestMethodTest.java)

## surface/mcp

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/mcp/CompositeResources.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/CompositeResources.java)
- [java/ai/protomolt/proto/mcp/DelegationResources.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/DelegationResources.java)
- [java/ai/protomolt/proto/mcp/JsonRpc.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/JsonRpc.java)
- [java/ai/protomolt/proto/mcp/McpMain.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/McpMain.java)
- [java/ai/protomolt/proto/mcp/McpResources.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/McpResources.java)
- [java/ai/protomolt/proto/mcp/McpServer.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/McpServer.java)
- [java/ai/protomolt/proto/mcp/RegistryResources.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/RegistryResources.java)
- [java/ai/protomolt/proto/mcp/ResourceAdapters.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/ResourceAdapters.java)
- [java/ai/protomolt/proto/mcp/ServiceProfileResources.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/ServiceProfileResources.java)
- [java/ai/protomolt/proto/mcp/WorkspaceResources.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/WorkspaceResources.java)
- [java/ai/protomolt/proto/mcp/package-info.java](../../surface/mcp/src/main/java/ai/protomolt/proto/mcp/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/mcp/DelegationMcpAcceptanceTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/DelegationMcpAcceptanceTest.java)
- [java/ai/protomolt/proto/mcp/DelegationMcpRestartTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/DelegationMcpRestartTest.java)
- [java/ai/protomolt/proto/mcp/DelegationResourcesTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/DelegationResourcesTest.java)
- [java/ai/protomolt/proto/mcp/JsonRpcTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/JsonRpcTest.java)
- [java/ai/protomolt/proto/mcp/McpLifecycleTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/McpLifecycleTest.java)
- [java/ai/protomolt/proto/mcp/McpMainTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/McpMainTest.java)
- [java/ai/protomolt/proto/mcp/McpMoonshotDialectTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/McpMoonshotDialectTest.java)
- [java/ai/protomolt/proto/mcp/McpScopeTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/McpScopeTest.java)
- [java/ai/protomolt/proto/mcp/McpServerEdgeCasesTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/McpServerEdgeCasesTest.java)
- [java/ai/protomolt/proto/mcp/McpServerTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/McpServerTest.java)
- [java/ai/protomolt/proto/mcp/RegistryResourcesDocumentTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/RegistryResourcesDocumentTest.java)
- [java/ai/protomolt/proto/mcp/RegistryResourcesTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/RegistryResourcesTest.java)
- [java/ai/protomolt/proto/mcp/SequentialRequestIdTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/SequentialRequestIdTest.java)
- [java/ai/protomolt/proto/mcp/ServiceProfileResourcesTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/ServiceProfileResourcesTest.java)
- [java/ai/protomolt/proto/mcp/StdioRoundTripTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/StdioRoundTripTest.java)
- [java/ai/protomolt/proto/mcp/WorkspaceResourcesTest.java](../../surface/mcp/src/test/java/ai/protomolt/proto/mcp/WorkspaceResourcesTest.java)

## surface/mcp/transport

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/mcp/transport/CompositeResources.java](../../surface/mcp/transport/src/main/java/ai/protomolt/proto/mcp/transport/CompositeResources.java)
- [java/ai/protomolt/proto/mcp/transport/JsonRpc.java](../../surface/mcp/transport/src/main/java/ai/protomolt/proto/mcp/transport/JsonRpc.java)
- [java/ai/protomolt/proto/mcp/transport/McpResources.java](../../surface/mcp/transport/src/main/java/ai/protomolt/proto/mcp/transport/McpResources.java)
- [java/ai/protomolt/proto/mcp/transport/McpServer.java](../../surface/mcp/transport/src/main/java/ai/protomolt/proto/mcp/transport/McpServer.java)
- [java/ai/protomolt/proto/mcp/transport/WorkspaceResources.java](../../surface/mcp/transport/src/main/java/ai/protomolt/proto/mcp/transport/WorkspaceResources.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/mcp/transport/MinimalTransportTest.java](../../surface/mcp/transport/src/test/java/ai/protomolt/proto/mcp/transport/MinimalTransportTest.java)
- [java/ai/protomolt/proto/mcp/transport/SessionFailureTest.java](../../surface/mcp/transport/src/test/java/ai/protomolt/proto/mcp/transport/SessionFailureTest.java)

## transform/delegation/contract

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/delegation/contract/DelegationValidation.java](../../transform/delegation/contract/src/main/java/ai/protomolt/proto/delegation/contract/DelegationValidation.java)
- [java/ai/protomolt/proto/delegation/contract/DeliverableContracts.java](../../transform/delegation/contract/src/main/java/ai/protomolt/proto/delegation/contract/DeliverableContracts.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/delegation/contract/StandaloneContractTest.java](../../transform/delegation/contract/src/test/java/ai/protomolt/proto/delegation/contract/StandaloneContractTest.java)
- [java/ai/protomolt/proto/delegation/contract/StandaloneDeliverableTest.java](../../transform/delegation/contract/src/test/java/ai/protomolt/proto/delegation/contract/StandaloneDeliverableTest.java)

## transform/mapper/cel

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/mapper/cel/CelMappingRule.java](../../transform/mapper/cel/src/main/java/ai/protomolt/proto/mapper/cel/CelMappingRule.java)
- [java/ai/protomolt/proto/mapper/cel/CelProtoMapper.java](../../transform/mapper/cel/src/main/java/ai/protomolt/proto/mapper/cel/CelProtoMapper.java)
- [java/ai/protomolt/proto/mapper/cel/package-info.java](../../transform/mapper/cel/src/main/java/ai/protomolt/proto/mapper/cel/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/mapper/cel/CelFilterSelectorTest.java](../../transform/mapper/cel/src/test/java/ai/protomolt/proto/mapper/cel/CelFilterSelectorTest.java)
- [java/ai/protomolt/proto/mapper/cel/CelFixtures.java](../../transform/mapper/cel/src/test/java/ai/protomolt/proto/mapper/cel/CelFixtures.java)
- [java/ai/protomolt/proto/mapper/cel/CelMappingRuleTest.java](../../transform/mapper/cel/src/test/java/ai/protomolt/proto/mapper/cel/CelMappingRuleTest.java)
- [java/ai/protomolt/proto/mapper/cel/CelMappingUseCasesTest.java](../../transform/mapper/cel/src/test/java/ai/protomolt/proto/mapper/cel/CelMappingUseCasesTest.java)
- [java/ai/protomolt/proto/mapper/cel/CelProtoMapperContractTest.java](../../transform/mapper/cel/src/test/java/ai/protomolt/proto/mapper/cel/CelProtoMapperContractTest.java)
- [java/ai/protomolt/proto/mapper/cel/CelProtoMapperTest.java](../../transform/mapper/cel/src/test/java/ai/protomolt/proto/mapper/cel/CelProtoMapperTest.java)

## transform/mapper/core

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/mapper/MappingException.java](../../transform/mapper/core/src/main/java/ai/protomolt/proto/mapper/MappingException.java)
- [java/ai/protomolt/proto/mapper/ProtoFieldMapper.java](../../transform/mapper/core/src/main/java/ai/protomolt/proto/mapper/ProtoFieldMapper.java)
- [java/ai/protomolt/proto/mapper/ProtoFieldMapperImpl.java](../../transform/mapper/core/src/main/java/ai/protomolt/proto/mapper/ProtoFieldMapperImpl.java)
- [java/ai/protomolt/proto/mapper/TextMappingRule.java](../../transform/mapper/core/src/main/java/ai/protomolt/proto/mapper/TextMappingRule.java)
- [java/ai/protomolt/proto/mapper/TextRuleParser.java](../../transform/mapper/core/src/main/java/ai/protomolt/proto/mapper/TextRuleParser.java)
- [java/ai/protomolt/proto/mapper/package-info.java](../../transform/mapper/core/src/main/java/ai/protomolt/proto/mapper/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/mapper/DynamicMessageStructTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/DynamicMessageStructTest.java)
- [java/ai/protomolt/proto/mapper/IncludeDefaultsTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/IncludeDefaultsTest.java)
- [java/ai/protomolt/proto/mapper/LiteralValueReadTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/LiteralValueReadTest.java)
- [java/ai/protomolt/proto/mapper/MapInPlaceProgressiveTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/MapInPlaceProgressiveTest.java)
- [java/ai/protomolt/proto/mapper/MapSourceTargetTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/MapSourceTargetTest.java)
- [java/ai/protomolt/proto/mapper/MappingExceptionTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/MappingExceptionTest.java)
- [java/ai/protomolt/proto/mapper/PathErrorSemanticsTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/PathErrorSemanticsTest.java)
- [java/ai/protomolt/proto/mapper/ProtoFieldMapperEnhancedTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/ProtoFieldMapperEnhancedTest.java)
- [java/ai/protomolt/proto/mapper/ProtoFieldMapperRegressionTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/ProtoFieldMapperRegressionTest.java)
- [java/ai/protomolt/proto/mapper/ProtoFieldMapperTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/ProtoFieldMapperTest.java)
- [java/ai/protomolt/proto/mapper/ProtoFieldMapperUseCasesTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/ProtoFieldMapperUseCasesTest.java)
- [java/ai/protomolt/proto/mapper/SetValueConversionTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/SetValueConversionTest.java)
- [java/ai/protomolt/proto/mapper/TestDescriptors.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/TestDescriptors.java)
- [java/ai/protomolt/proto/mapper/TextRuleParserTest.java](../../transform/mapper/core/src/test/java/ai/protomolt/proto/mapper/TextRuleParserTest.java)

## transform/mapper/metadata

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/mapper/metadata/MetadataExtractor.java](../../transform/mapper/metadata/src/main/java/ai/protomolt/proto/mapper/metadata/MetadataExtractor.java)
- [java/ai/protomolt/proto/mapper/metadata/package-info.java](../../transform/mapper/metadata/src/main/java/ai/protomolt/proto/mapper/metadata/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/mapper/metadata/MetadataExtractorContractTest.java](../../transform/mapper/metadata/src/test/java/ai/protomolt/proto/mapper/metadata/MetadataExtractorContractTest.java)
- [java/ai/protomolt/proto/mapper/metadata/MetadataExtractorTest.java](../../transform/mapper/metadata/src/test/java/ai/protomolt/proto/mapper/metadata/MetadataExtractorTest.java)

## transform/pipeline

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/pipeline/DescriptorSets.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/DescriptorSets.java)
- [java/ai/protomolt/proto/pipeline/DynamicPipelineTransport.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/DynamicPipelineTransport.java)
- [java/ai/protomolt/proto/pipeline/PipelineChecker.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/PipelineChecker.java)
- [java/ai/protomolt/proto/pipeline/PipelineExecutor.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/PipelineExecutor.java)
- [java/ai/protomolt/proto/pipeline/PipelineTransport.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/PipelineTransport.java)
- [java/ai/protomolt/proto/pipeline/PipelineValidation.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/PipelineValidation.java)
- [java/ai/protomolt/proto/pipeline/StreamPaths.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/StreamPaths.java)
- [java/ai/protomolt/proto/pipeline/WorkflowPipelineCompiler.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/WorkflowPipelineCompiler.java)
- [java/ai/protomolt/proto/pipeline/package-info.java](../../transform/pipeline/src/main/java/ai/protomolt/proto/pipeline/package-info.java)
- [proto/ai/protomolt/proto/pipeline/v1/pipeline.proto](../../transform/pipeline/src/main/proto/ai/protomolt/proto/pipeline/v1/pipeline.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/pipeline/PipelineCheckerCardinalityTest.java](../../transform/pipeline/src/test/java/ai/protomolt/proto/pipeline/PipelineCheckerCardinalityTest.java)
- [java/ai/protomolt/proto/pipeline/PipelineCheckerMappingTest.java](../../transform/pipeline/src/test/java/ai/protomolt/proto/pipeline/PipelineCheckerMappingTest.java)
- [java/ai/protomolt/proto/pipeline/PipelineCheckerShapeTest.java](../../transform/pipeline/src/test/java/ai/protomolt/proto/pipeline/PipelineCheckerShapeTest.java)
- [java/ai/protomolt/proto/pipeline/PipelineExecutorTest.java](../../transform/pipeline/src/test/java/ai/protomolt/proto/pipeline/PipelineExecutorTest.java)
- [java/ai/protomolt/proto/pipeline/PipelineFixtures.java](../../transform/pipeline/src/test/java/ai/protomolt/proto/pipeline/PipelineFixtures.java)
- [java/ai/protomolt/proto/pipeline/PipelineValidationTest.java](../../transform/pipeline/src/test/java/ai/protomolt/proto/pipeline/PipelineValidationTest.java)
- [java/ai/protomolt/proto/pipeline/WorkflowPipelineCompilerTest.java](../../transform/pipeline/src/test/java/ai/protomolt/proto/pipeline/WorkflowPipelineCompilerTest.java)

## transform/projection

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/projection/MessageProjection.java](../../transform/projection/src/main/java/ai/protomolt/proto/projection/MessageProjection.java)
- [java/ai/protomolt/proto/projection/ProjectionException.java](../../transform/projection/src/main/java/ai/protomolt/proto/projection/ProjectionException.java)
- [java/ai/protomolt/proto/projection/SourceResolver.java](../../transform/projection/src/main/java/ai/protomolt/proto/projection/SourceResolver.java)
- [java/ai/protomolt/proto/projection/package-info.java](../../transform/projection/src/main/java/ai/protomolt/proto/projection/package-info.java)
- [proto/ai/protomolt/proto/projection/v1/projection.proto](../../transform/projection/src/main/proto/ai/protomolt/proto/projection/v1/projection.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/projection/MessageProjectionTest.java](../../transform/projection/src/test/java/ai/protomolt/proto/projection/MessageProjectionTest.java)
- [java/ai/protomolt/proto/projection/ProjectionSemanticsTest.java](../../transform/projection/src/test/java/ai/protomolt/proto/projection/ProjectionSemanticsTest.java)
- [proto/ai/protomolt/proto/projection/test/v1/projection_test.proto](../../transform/projection/src/test/proto/ai/protomolt/proto/projection/test/v1/projection_test.proto)

## transform/receipt

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/receipt/RecordKeys.java](../../transform/receipt/src/main/java/ai/protomolt/proto/receipt/RecordKeys.java)
- [java/ai/protomolt/proto/receipt/RecordSigner.java](../../transform/receipt/src/main/java/ai/protomolt/proto/receipt/RecordSigner.java)
- [java/ai/protomolt/proto/receipt/RecordVerifier.java](../../transform/receipt/src/main/java/ai/protomolt/proto/receipt/RecordVerifier.java)
- [java/ai/protomolt/proto/receipt/TrustSnapshots.java](../../transform/receipt/src/main/java/ai/protomolt/proto/receipt/TrustSnapshots.java)
- [java/ai/protomolt/proto/receipt/Verification.java](../../transform/receipt/src/main/java/ai/protomolt/proto/receipt/Verification.java)
- [java/ai/protomolt/proto/receipt/WorkRecords.java](../../transform/receipt/src/main/java/ai/protomolt/proto/receipt/WorkRecords.java)
- [proto/ai/protomolt/proto/receipt/v1/receipt.proto](../../transform/receipt/src/main/proto/ai/protomolt/proto/receipt/v1/receipt.proto)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/receipt/ConformanceTest.java](../../transform/receipt/src/test/java/ai/protomolt/proto/receipt/ConformanceTest.java)
- [java/ai/protomolt/proto/receipt/RecordKeysTest.java](../../transform/receipt/src/test/java/ai/protomolt/proto/receipt/RecordKeysTest.java)
- [java/ai/protomolt/proto/receipt/RecordSignerTest.java](../../transform/receipt/src/test/java/ai/protomolt/proto/receipt/RecordSignerTest.java)
- [java/ai/protomolt/proto/receipt/RecordVerifierTest.java](../../transform/receipt/src/test/java/ai/protomolt/proto/receipt/RecordVerifierTest.java)
- [java/ai/protomolt/proto/receipt/TrustSnapshotLoadTest.java](../../transform/receipt/src/test/java/ai/protomolt/proto/receipt/TrustSnapshotLoadTest.java)
- [java/ai/protomolt/proto/receipt/WorkRecordLogTest.java](../../transform/receipt/src/test/java/ai/protomolt/proto/receipt/WorkRecordLogTest.java)
- [java/ai/protomolt/proto/receipt/WorkRecordsTest.java](../../transform/receipt/src/test/java/ai/protomolt/proto/receipt/WorkRecordsTest.java)

## transform/shapes

### Production sources, contracts and SPI registrations

- [java/ai/protomolt/proto/shapes/Literals.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/Literals.java)
- [java/ai/protomolt/proto/shapes/MappingSuggester.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/MappingSuggester.java)
- [java/ai/protomolt/proto/shapes/MessageJoiner.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/MessageJoiner.java)
- [java/ai/protomolt/proto/shapes/MessageScope.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/MessageScope.java)
- [java/ai/protomolt/proto/shapes/RuleChecker.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/RuleChecker.java)
- [java/ai/protomolt/proto/shapes/SchemaInferrer.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/SchemaInferrer.java)
- [java/ai/protomolt/proto/shapes/SchemaMerger.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/SchemaMerger.java)
- [java/ai/protomolt/proto/shapes/ScopedProtoMapper.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/ScopedProtoMapper.java)
- [java/ai/protomolt/proto/shapes/ShapeSynthesizer.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/ShapeSynthesizer.java)
- [java/ai/protomolt/proto/shapes/package-info.java](../../transform/shapes/src/main/java/ai/protomolt/proto/shapes/package-info.java)

### Existing test sources (not executed in this audit)

- [java/ai/protomolt/proto/shapes/LiteralsTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/LiteralsTest.java)
- [java/ai/protomolt/proto/shapes/MappingSuggesterTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/MappingSuggesterTest.java)
- [java/ai/protomolt/proto/shapes/MessageJoinerTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/MessageJoinerTest.java)
- [java/ai/protomolt/proto/shapes/MessageScopeTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/MessageScopeTest.java)
- [java/ai/protomolt/proto/shapes/RuleCheckerTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/RuleCheckerTest.java)
- [java/ai/protomolt/proto/shapes/SchemaInferrerEdgeCasesTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/SchemaInferrerEdgeCasesTest.java)
- [java/ai/protomolt/proto/shapes/SchemaInferrerTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/SchemaInferrerTest.java)
- [java/ai/protomolt/proto/shapes/SchemaMergerEdgeCasesTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/SchemaMergerEdgeCasesTest.java)
- [java/ai/protomolt/proto/shapes/SchemaMergerTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/SchemaMergerTest.java)
- [java/ai/protomolt/proto/shapes/ScopedProtoMapperTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/ScopedProtoMapperTest.java)
- [java/ai/protomolt/proto/shapes/ShapeSynthesisTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/ShapeSynthesisTest.java)
- [java/ai/protomolt/proto/shapes/ShapeSynthesizerEdgeCasesTest.java](../../transform/shapes/src/test/java/ai/protomolt/proto/shapes/ShapeSynthesizerEdgeCasesTest.java)

