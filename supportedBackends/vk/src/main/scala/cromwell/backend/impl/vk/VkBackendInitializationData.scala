package cromwell.backend.impl.vk

import cromwell.backend.standard.{StandardInitializationData, StandardValidatedRuntimeAttributesBuilder}

case class VkBackendInitializationData
(
  override val workflowPaths: VkWorkflowPaths,
  override val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder,
  vkConfiguration: VkConfiguration
) extends StandardInitializationData(workflowPaths, runtimeAttributesBuilder, classOf[VkExpressionFunctions])
