package cromwell.backend.impl.spark

import java.nio.file.attribute.PosixFilePermission

import akka.actor.{ActorContext, ActorRef, Props}
import common.exception.MessageAggregation
import common.validation.Validation._
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionResponse, JobFailedNonRetryableResponse, JobSucceededResponse, RunOnBackend}
import cromwell.backend._
import cromwell.backend.impl.spark.SparkClusterProcess._
import cromwell.backend.io.JobPathsWithDocker
import cromwell.backend.sfs.{SharedFileSystem, SharedFileSystemExpressionFunctions}
import cromwell.backend.Command
import cromwell.backend.OutputEvaluator.{InvalidJobOutputs, JobOutputsEvaluationException, ValidJobOutputs}
import cromwell.core.path.JavaWriterImplicits._
import cromwell.core.path.Obsolete._
import cromwell.core.path.{DefaultPathBuilder, TailedWriter, UntailedWriter}

import scala.concurrent.{Future, Promise}
import scala.sys.process.ProcessLogger
import scala.util.{Failure, Success, Try}

object SparkJobExecutionActor {
  val DefaultPathBuilders = List(DefaultPathBuilder)

  def props(jobDescriptor: BackendJobDescriptor,
            configurationDescriptor: BackendConfigurationDescriptor,
            ioActorProxy: ActorRef): Props =
    Props(new SparkJobExecutionActor(jobDescriptor, configurationDescriptor, ioActorProxy))
}

class SparkJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                             override val configurationDescriptor: BackendConfigurationDescriptor,
                             ioActorProxy: ActorRef) extends BackendJobExecutionActor with SharedFileSystem {

  import SparkJobExecutionActor._

  override val pathBuilders = DefaultPathBuilders
  private val tag = s"SparkJobExecutionActor-${jobDescriptor.key.tag}:"

  implicit def actorContext: ActorContext = this.context

  lazy val cmds = new SparkCommands
  lazy val clusterExtProcess = new SparkClusterProcess()(context.system)

  lazy val extProcess = new SparkProcess {}
  lazy val clusterManagerConfig = configurationDescriptor.backendConfig.getConfig("cluster-manager")
  private val fileSystemsConfig = configurationDescriptor.backendConfig.getConfig("filesystems")
  private val sparkMaster = configurationDescriptor.backendConfig.getString("master").toLowerCase
  private val sparkDeployMode = configurationDescriptor.backendConfig.getString("deployMode").toLowerCase
  override val sharedFileSystemConfig = fileSystemsConfig.getConfig("local")
  private val workflowDescriptor = jobDescriptor.workflowDescriptor
  private val jobPaths = JobPathsWithDocker(jobDescriptor.key, workflowDescriptor, configurationDescriptor.backendConfig)

  // Files
  private val executionDir = jobPaths.callExecutionRoot
  private val scriptPath = jobPaths.script

  private lazy val standardPaths = jobPaths.standardPaths
  private lazy val stdoutWriter = extProcess.untailedWriter(standardPaths.output)
  private lazy val stderrWriter = extProcess.tailedWriter(100, standardPaths.error)

  private lazy val clusterStdoutWriter = clusterExtProcess.untailedWriter(standardPaths.output)
  private lazy val clusterStderrWriter = clusterExtProcess.tailedWriter(100, standardPaths.error)
  private lazy val SubmitJobJson = "%s.json"
  private lazy val isClusterMode = isSparkClusterMode(sparkDeployMode, sparkMaster)

  private val callEngineFunction = SharedFileSystemExpressionFunctions(jobPaths, DefaultPathBuilders, ioActorProxy, context.dispatcher)

  private val executionResponse = Promise[BackendJobExecutionResponse]()

  private val runtimeAttributes = {
    SparkRuntimeAttributes(jobDescriptor.runtimeAttributes, jobDescriptor.workflowDescriptor.workflowOptions)
  }

  /**
    * Restart or resume a previously-started job.
    */
  override def recover: Future[BackendJobExecutionResponse] = {
    log.warning("{} Spark backend currently doesn't support recovering jobs. Starting {} again.", tag, jobDescriptor.key.call.localName)
    taskLauncher
    executionResponse.future
  }

  /**
    * Execute a new job.
    */
  override def execute: Future[BackendJobExecutionResponse] = {
    createExecutionFolderAndScript()
    taskLauncher
    executionResponse.future
  }

  private def executeTask(process: SparkProcess, stdoutWriter: UntailedWriter, stderrWriter: TailedWriter): Future[BackendJobExecutionResponse] = {
    val submitResult = submitSparkScript(process, ProcessLogger(stdoutWriter.writeWithNewline, stderrWriter.writeWithNewline))
    List(stdoutWriter.writer, stderrWriter.writer).foreach(_.flushAndClose())

    resolveExecutionResult(submitResult, runtimeAttributes.failOnStderr)
  }

  private def submitSparkScript(sparkProcess: SparkProcess, processLogger: ProcessLogger): Try[Int] = {
    val argv = List("sh", scriptPath.toString)
    val process = sparkProcess.externalProcess(argv, processLogger)
    val jobReturnCode = Try(process.exitValue()) // blocks until process (i.e. spark submission) finishes
    log.debug("{} Return code of spark submit command: {}", tag, jobReturnCode)
    jobReturnCode
  }

  private def resolveExecutionResult(jobReturnCode: Try[Int], failedOnStderr: Boolean): Future[BackendJobExecutionResponse] = {
    (jobReturnCode, failedOnStderr) match {
      case (Success(0), true) if File(standardPaths.error).lines.toList.nonEmpty =>
        Future.successful(JobFailedNonRetryableResponse(jobDescriptor.key,
          new IllegalStateException(s"Execution process failed although return code is zero but stderr is not empty"), Option(0)))
      case (Success(0), _) => resolveExecutionProcess
      case (Success(rc), _) => Future.successful(JobFailedNonRetryableResponse(jobDescriptor.key,
        new IllegalStateException(s"Execution process failed. Spark returned non zero status code: $rc"), Option(rc)))
      case (Failure(error), _) => Future.successful(JobFailedNonRetryableResponse(jobDescriptor.key, error, None))
    }

  }

  private def resolveExecutionProcess: Future[BackendJobExecutionResponse] = {
    isClusterMode match {
      case true =>
        clusterExtProcess.startMonitoringSparkClusterJob(jobPaths.callExecutionRoot, SubmitJobJson.format(sparkDeployMode)) collect {
          case Finished => processSuccess(0)
          case Failed(error: Throwable) => JobFailedNonRetryableResponse(jobDescriptor.key, error, None)
        } recover {
          case error: Throwable => JobFailedNonRetryableResponse(jobDescriptor.key, error, None)
        }
      case false => Future.successful(processSuccess(0))
    }
  }

  private def processSuccess(rc: Int) = {
    evaluateOutputs(callEngineFunction, outputMapper(jobPaths)) match {
      case ValidJobOutputs(outputs) => JobSucceededResponse(jobDescriptor.key, Some(rc), outputs, None, Seq.empty, dockerImageUsed = None, resultGenerationMode = RunOnBackend)
      case InvalidJobOutputs(evaluationErrors) =>
        val exception = new MessageAggregation {
          override def exceptionContext: String = "Failed post processing of outputs"
          override def errorMessages: Traversable[String] = evaluationErrors.toList
        }
        JobFailedNonRetryableResponse(jobDescriptor.key, exception, Option(rc))
      case JobOutputsEvaluationException(evaluationException) =>
        val message = Option(evaluationException.getMessage) map {
          ": " + _
        } getOrElse ""
        JobFailedNonRetryableResponse(jobDescriptor.key, new Throwable("Failed post processing of outputs" + message, evaluationException), Option(rc))
    }
  }

  /**
    * Abort a running job.
    */
  // -Ywarn-value-discard
  // override def abort(): Unit = Future.failed(new UnsupportedOperationException("SparkBackend currently doesn't support aborting jobs."))
  override def abort(): Unit = throw new UnsupportedOperationException("SparkBackend currently doesn't support aborting jobs.")


  private def createExecutionFolderAndScript(): Unit = {
    try {
      log.debug("{} Creating execution folder: {}", tag, executionDir)
      executionDir.toString.toFile.createIfNotExists(asDirectory = true, createParents = true)

      log.debug("{} Resolving job command", tag)

      val command = Command.instantiate(
        jobDescriptor,
        callEngineFunction,
        localizeInputs(jobPaths.callInputsRoot, docker = false),
        valueMapper = identity,
        runtimeEnvironment = RuntimeEnvironmentBuilder(jobDescriptor.runtimeAttributes, jobPaths)(MinimumRuntimeSettings())
      )

      log.debug("{} Creating bash script for executing command: {}", tag, command)
      // TODO: we should use shapeless Heterogeneous list here not good to have generic map
      val attributes: Map[String, Any] = Map(
        SparkCommands.AppMainClass -> runtimeAttributes.appMainClass.getOrElse(""),
        SparkCommands.Master -> sparkMaster,
        SparkCommands.ExecutorCores -> runtimeAttributes.executorCores,
        SparkCommands.ExecutorMemory -> runtimeAttributes.executorMemory.toString.replaceAll("\\s","").toLowerCase,
        SparkCommands.AdditionalArgs -> runtimeAttributes.additionalArgs.getOrElse(""),
        SparkCommands.SparkAppWithArgs -> command.toTry.get,
        SparkCommands.DeployMode -> sparkDeployMode
      )

      val sparkSubmitCmd = cmds.sparkSubmitCommand(attributes)
      val sparkCommand = if (isClusterMode) {
        sparkSubmitCmd.concat(" > %s 2>&1".format(SubmitJobJson.format(sparkDeployMode)))
      } else {
        sparkSubmitCmd
      }

      cmds.writeScript(sparkCommand, scriptPath, executionDir)
      File(scriptPath).addPermission(PosixFilePermission.OWNER_EXECUTE)
      ()

    } catch {
      case ex: Exception =>
        log.error(ex, "Failed to prepare task: " + ex.getMessage)
        // -Ywarn-value-discard
        // executionResponse success FailedNonRetryableResponse(jobDescriptor.key, ex, None)
        ()
    }
  }

  private def isSparkClusterMode(deployMode: String, master: String): Boolean = {
    // typical master value is spark://host-ip:6066 and deployMode also is cluster
    List("6066", "spark").exists(master.toLowerCase.contains) && deployMode.equals("cluster")
  }

  private def taskLauncher = {
    Try {
      isClusterMode match {
        case true => executionResponse completeWith executeTask(clusterExtProcess, clusterStdoutWriter, clusterStderrWriter)
        case false => executionResponse completeWith executeTask(extProcess, stdoutWriter, stderrWriter)
      }
    } recover {
      case exception => executionResponse success JobFailedNonRetryableResponse(jobDescriptor.key, exception, None)
    }
  }

}
