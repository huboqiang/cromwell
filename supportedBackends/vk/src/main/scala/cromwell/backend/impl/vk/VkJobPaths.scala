package cromwell.backend.impl.vk

import com.typesafe.config.Config
import cromwell.backend.io.{JobPaths, WorkflowPaths}
import cromwell.backend.{BackendJobDescriptorKey, BackendWorkflowDescriptor}
import cromwell.core.path._

object VkJobPaths {
  def apply(jobKey: BackendJobDescriptorKey,
  workflowDescriptor: BackendWorkflowDescriptor,
  config: Config,
  pathBuilders: List[PathBuilder] = WorkflowPaths.DefaultPathBuilders) = {
    val workflowPaths = VkWorkflowPaths(workflowDescriptor, config, pathBuilders)
    new VkJobPaths(workflowPaths, jobKey)
  }
}

case class VkJobPaths private[vk](override val workflowPaths: VkWorkflowPaths,
                                  jobKey: BackendJobDescriptorKey) extends JobPaths {

  import JobPaths._

  override lazy val callExecutionRoot = {
    callRoot.resolve("execution")
  }
  val callDockerRoot = callPathBuilder(workflowPaths.dockerWorkflowRoot, jobKey)
  val callExecutionDockerRoot = callDockerRoot.resolve("execution")
  val callInputsDockerRoot = callDockerRoot.resolve("inputs")
  val callInputsRoot = callRoot.resolve("inputs")

  // Given an output path, return a path localized to the storage file system
  def storageOutput(path: String): String = {
    callExecutionRoot.resolve(path).toString
  }

  // Given an output path, return a path localized to the container file system
  def containerOutput(cwd: Path, path: String): String = containerExec(cwd, path)

  // TODO this could be used to create a separate directory for outputs e.g.
  // callDockerRoot.resolve("outputs").resolve(name).toString

  // Given an file name, return a path localized to the container's execution directory
  def containerExec(cwd: Path, path: String): String = {
    cwd.resolve(path).toString
  }
}
