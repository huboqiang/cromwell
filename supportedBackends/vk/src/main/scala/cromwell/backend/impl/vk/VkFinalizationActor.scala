package cromwell.backend.impl.vk

import cromwell.backend.standard._
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendWorkflowDescriptor, JobExecutionMap}
import cromwell.core.CallOutputs
import wom.graph.CommandCallNode

import scala.concurrent.Future

case class VkFinalizationActorParams
(
  workflowDescriptor: BackendWorkflowDescriptor,
  calls: Set[CommandCallNode],
  jobExecutionMap: JobExecutionMap,
  workflowOutputs: CallOutputs,
  initializationDataOption: Option[BackendInitializationData],
  vkConfiguration: VkConfiguration,
  vkStatusManager: VkStatusManager
) extends StandardFinalizationActorParams {
  override val configurationDescriptor: BackendConfigurationDescriptor = vkConfiguration.configurationDescriptor
}

class VkFinalizationActor(params: VkFinalizationActorParams)
  extends StandardFinalizationActor(params) {

  override def afterAll(): Future[Unit] = {
    params.vkStatusManager.remove(workflowDescriptor.id)
    super.afterAll()
  }
}
