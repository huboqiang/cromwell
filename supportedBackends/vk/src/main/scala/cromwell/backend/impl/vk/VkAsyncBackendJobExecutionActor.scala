package cromwell.backend.impl.vk

import akka.actor.{ActorRef, ActorSystem}

import java.io.FileNotFoundException
import java.nio.file.FileAlreadyExistsException
import java.util.concurrent.ExecutionException
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import akka.http.scaladsl.model.{RequestEntity, _}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import cromwell.services.SuccessfulMetadataJsonResponse
import com.google.gson.{Gson, JsonObject, JsonParser}
import cromwell.backend.BackendJobLifecycleActor
import cromwell.backend.async.{AbortedExecutionHandle, ExecutionHandle, FailedNonRetryableExecutionHandle, FailedRetryableExecutionHandle, PendingExecutionHandle, RetryWithMoreMemory, ReturnCodeIsNotAnInt, StderrNonEmpty, WrongReturnCode}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob}
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.core.retry.{Retry, SimpleExponentialBackoff}
import wom.values.WomFile
import wom.values._
import common.collections.EnhancedCollections._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, _}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import skuber.batch.Job
import skuber.json.batch.format._
import wdl.draft2.model.FullyQualifiedName
import skuber.json.PlayJsonSupportForAkkaHttp._
import cromwell.core.{CromwellFatalExceptionMarker, WorkflowId}
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import org.apache.commons.lang3.exception.ExceptionUtils

sealed trait VkRunStatus {
  def isTerminal: Boolean
}

case object Running extends VkRunStatus {
  def isTerminal = false
}

case object Complete extends VkRunStatus {
  def isTerminal = true
}

case object FailedOrError extends VkRunStatus {
  def isTerminal = true
}

case object Cancelled extends VkRunStatus {
  def isTerminal = true
}

object VkAsyncBackendJobExecutionActor {
  val JobIdKey = "vk_job_id"
  val badSslConfig: AkkaSSLConfig = AkkaSSLConfig(ActorSystem.apply()).mapSettings(s =>
    s.withLoose(
      s.loose
        .withDisableHostnameVerification(true)
    )
  )
  val badCtx = Http(ActorSystem.apply()).createClientHttpsContext(badSslConfig)
}

class VkAsyncBackendJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams)
  extends BackendJobLifecycleActor with StandardAsyncExecutionActor with VkJobCachingActorHelper {
  implicit val actorSystem = context.system
  implicit val materializer = ActorMaterializer()

  implicit val dispatcher = actorSystem.dispatcher

  override type StandardAsyncRunInfo = Any

  override type StandardAsyncRunState = VkRunStatus

  def statusEquivalentTo(thiz: StandardAsyncRunState)(that: StandardAsyncRunState): Boolean = thiz == that

  override lazy val pollBackOff = SimpleExponentialBackoff(
    initialInterval = 10 seconds,
    maxInterval = 20 seconds,
    multiplier = 1.1,
    randomizationFactor = 0.1
  )

  override lazy val executeOrRecoverBackOff = SimpleExponentialBackoff(
    initialInterval = 20 seconds,
    maxInterval = 1 minutes,
    multiplier = 1.2,
    randomizationFactor = 0.3
  )

  override lazy val dockerImageUsed: Option[String] = Option(runtimeAttributes.dockerImage)

  private val workflowId = workflowDescriptor.rootWorkflowId.toString

  private val namespace = vkConfiguration.namespace

//  private val apiServerUrl = s"https://cci.${vkConfiguration.region}.myhuaweicloud.com"
  private val apiServerUrl = vkConfiguration.k8sURL

  override lazy val jobTag: String = jobDescriptor.key.tag
  private val gson = new Gson()
  override lazy val jobShell: String = workflowDescriptor.workflowOptions.getOrElse("system.job-shell", configurationDescriptor.globalConfig.getString("system.job-shell"))


  /**
    * Localizes the file.
    */
  override def preProcessWomFile(womFile: WomFile): WomFile = {
    getPath(womFile.value) match {
      case Success(path: Path) if path.uri.getScheme.equals("obs") => womFile
      case _ => sharedFileSystem.localizeWomFile(vkJobPaths.callInputsRoot, false)(womFile)
    }
  }

  override def mapCommandLineWomFile(womFile: WomFile): WomFile = {
    womFile.mapFile(value =>
      (getPath(value), asAdHocFile(womFile)) match {
        case (Success(path: Path), Some(adHocFile)) =>
          // Ad hoc files will be placed directly at the root ("/cromwell_root/ad_hoc_file.txt") unlike other input files
          // for which the full path is being propagated ("/cromwell_root/path/to/input_file.txt")
          vkJobPaths.containerExec(commandDirectory, adHocFile.alternativeName.getOrElse(path.name))
        case _ => mapCommandLineJobInputWomFile(womFile).value
      }
    )
  }

  override def mapCommandLineJobInputWomFile(womFile: WomFile): WomFile = {
    womFile.mapFile(value =>
      getPath(value) match {
        case Success(path: Path) if path.startsWith(vkJobPaths.workflowPaths.DockerRoot) =>
          path.pathAsString
        case Success(path: Path) if path.equals(vkJobPaths.callExecutionRoot) =>
          commandDirectory.pathAsString
        case Success(path: Path) if path.startsWith(vkJobPaths.callExecutionRoot) =>
          vkJobPaths.containerExec(commandDirectory, path.name)
        case Success(path: Path) if path.startsWith(vkJobPaths.callRoot) =>
          vkJobPaths.callDockerRoot.resolve(value.substring(vkJobPaths.callRoot.pathAsString.length+1)).pathAsString
        case Success(path: Path) =>
          vkJobPaths.callInputsDockerRoot.resolve(path.pathWithoutScheme.stripPrefix("/")).pathAsString
        case _ =>
          value
      }
    )
  }

  override lazy val commandDirectory: Path = {
    runtimeAttributes.dockerWorkingDir match {
      case Some(path) => DefaultPathBuilder.get(path)
      case None => vkJobPaths.callExecutionDockerRoot
    }
  }

  def createTaskMessage(): Future[Job] = {
    val task = VkTask(
      jobDescriptor,
      configurationDescriptor,
      vkJobPaths,
      runtimeAttributes,
      commandDirectory,
      dockerImageUsed.get,
      jobShell,
      vkConfiguration
    )
    Future.successful(task.job)
  }

  def writeScriptFile(): Future[Unit] = {
    commandScriptContents.fold(
      errors => Future.failed(new RuntimeException(errors.toList.mkString(", "))),
      asyncIo.writeAsync(jobPaths.script, _, Seq.empty)
    )
  }

  private def writeFunctionFiles: Map[FullyQualifiedName, Seq[WomFile]] =
    instantiatedCommand.createdFiles map { f => f.file.value.md5SumShort -> List(f.file) } toMap

  private val callInputFiles: Map[FullyQualifiedName, Seq[WomFile]] = jobDescriptor
    .fullyQualifiedInputs
    .safeMapValues {
      _.collectAsSeq { case w: WomFile => w }
    }

  private def checkInputs() = {
    var copies = Map[String, Future[Unit]]()
    (callInputFiles ++ writeFunctionFiles).flatMap {
      case (_, files) => files.flatMap(_.flattenFiles).zipWithIndex.map {
        case (f, _) =>
          getPath(f.value) match {
            case Success(path: Path) if path.uri.getScheme.equals("obs") =>
              val destination = vkJobPaths.callInputsRoot.resolve(path.pathWithoutScheme.stripPrefix("/"))
              if (!destination.exists && !copies.contains(destination.pathAsString)) {
                val future = asyncIo.copyAsync(path, destination)
                copies += (destination.pathAsString -> future)
              }
            case Success(path: Path) if !path.exists =>
              val source = getPath(path.pathAsString.replace(vkJobPaths.workflowPaths.executionRoot.pathAsString, vkConfiguration.storagePath.get)).get
              val future = asyncIo.copyAsync(source, path)
              copies += (path.pathAsString -> future)
            case _ =>
              Nil
          }
      }
    }
    for(future <- copies.values.toList){
      Await.result(future, Duration.Inf)
    }
  }



  override def executeAsync(): Future[ExecutionHandle] = {
    // create call exec dir
    vkJobPaths.callExecutionRoot.createPermissionedDirectories()
    try {
      checkInputs()

    val taskMessageFuture = createTaskMessage()
      try{
        for {
          _ <- writeScriptFile()
          taskMessage <- taskMessageFuture
          entity <- Marshal(taskMessage).to[RequestEntity]
          ctr <- makeRequest[Job](HttpRequest(method = HttpMethods.POST,
            headers = List(RawHeader("X-Auth-Token", vkConfiguration.token.getValue())),
            uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs",
            entity = transEntity(entity)))
        } yield {
          vkStatusManager.setStatus(workflowId, ctr.name, gson.toJsonTree(ctr).getAsJsonObject)
          PendingExecutionHandle(jobDescriptor, StandardAsyncJob(ctr.name), None, previousState = None)
        }
      } catch {
        case ex: Exception => Future.successful(FailedRetryableExecutionHandle(ex, kvPairsToSave = None))
        case t: Throwable => throw t
      }
    } catch {
      case t: Throwable => Future.successful(FailedNonRetryableExecutionHandle(t, kvPairsToSave = None))
    }
  }

  def transEntity(entity: RequestEntity): RequestEntity = {
    if(runtimeAttributes.disks.isEmpty){
      entity
    } else {
      val flow = Flow.fromFunction[ByteString, ByteString](source => {
        val jsonObject = JsonParser.parseString(source.utf8String)
        val volumes = jsonObject.getAsJsonObject.get("spec").getAsJsonObject.get("template").getAsJsonObject.get("spec").getAsJsonObject.get("volumes").getAsJsonArray
        if(vkConfiguration.isCCI){
          for(disk <- runtimeAttributes.disks.get){
            val emptyDir = s"""{"name":"${disk.name}","emptyDir":{"medium":${disk.diskType.hwsTypeName},"sizeLimit":${disk.sizeGb}Gi}}"""
            volumes.add(JsonParser.parseString(emptyDir))
          }
        }
        ByteString.fromString(jsonObject.toString, "utf-8")
      })
      entity.transformDataBytes(flow)
    }
  }

  def restartJob(jobName: String) = {
    pollStatusAsync(jobName) onComplete  {
      case Success(_) => tryAbort(StandardAsyncJob(jobName))
      case Failure(_) => ()
    }
  }

  override def reconnectAsync(jobId: StandardAsyncJob) = {
    val handle = PendingExecutionHandle[StandardAsyncJob, StandardAsyncRunInfo, StandardAsyncRunState](jobDescriptor, jobId, None, previousState = None)
    Future.successful(handle)
  }

  override def recoverAsync(jobId: StandardAsyncJob) = reconnectAsync(jobId)

  override def reconnectToAbortAsync(jobId: StandardAsyncJob) = {
    tryAbort(jobId)
    reconnectAsync(jobId)
  }

  override def tryAbort(job: StandardAsyncJob): Unit = {

    val returnCodeTmp = jobPaths.returnCode.plusExt("kill")
    returnCodeTmp.write(s"$SIGTERM\n")
    try {
      returnCodeTmp.moveTo(jobPaths.returnCode)
    } catch {
      case _: FileAlreadyExistsException =>
        // If the process has already completed, there will be an existing rc file.
        returnCodeTmp.delete(true)
    }
    vkStatusManager.TryDeleteVkJob(job.jobId)
    jobLogger.info("{} Aborted {} done", tag: Any, job.jobId)
  }

  override def requestsAbortAndDiesImmediately: Boolean = false

  override def pollStatusAsync(handle: StandardAsyncPendingExecutionHandle): Future[VkRunStatus] = {
    pollStatusAsync(handle.pendingJob.jobId)
  }

  private def pollStatusAsync(jobName: String): Future[VkRunStatus] = {
    var job = vkStatusManager.getStatus(workflowId, jobName)
    var timeNoutFound = 0
    while (job.isEmpty){
      val status = vkStatusManager.getWorkflowStatus(jobDescriptor.workflowDescriptor.id, serviceRegistryActor)
      if (status == "Aborting" || status == "Aborted"|| status == "Failed"){
        jobLogger.info(s"Job ${jobName} with status ${status} cannot be fetched. Canceled")
        return Future.successful(Cancelled)
      }

      println(s"Job ${jobName} not found, re-fetch ${timeNoutFound} of 240 every 15 seconds...")
      Thread.sleep(15000)
      job = vkStatusManager.getStatus(workflowId, jobName)
      timeNoutFound += 1
      if (timeNoutFound > 240){
          jobLogger.info(s"Job ${jobName} with status ${status} but K8S job cannot be detected for over 60-minutes. Failed.")
          return Future.successful(FailedOrError)
      }
    }

    val vkRunStatus = {
      val status = job.get.get("status")
      if (status.isJsonNull) {
        Running
      } else {
        status.getAsJsonObject match {
          case s if getVal(s, "failed").getOrElse(0) > 0 =>
            jobLogger.info(s"VK reported an error for Job ${jobName}: '$s'")
            FailedOrError
          case s if getVal(s, "succeeded").getOrElse(0) > 0 =>
            jobLogger.info(s"Job ${jobName} is complete")
            // Let bce-apiserver delete all finished jobs so that bce can get the pod actual running time.
            // vkStatusManager.deleteVkJob(jobName)
            Complete

          case s if getVal(s, "active").getOrElse(1) == 0 =>
            jobLogger.info(s"Job ${jobName} was canceled")
            // vkStatusManager.deleteVkJob(jobName)
            Cancelled

          case _ => Running
        }
      }
    }
    Future.successful(vkRunStatus)
  }

  private def getVal(jsObject: JsonObject, key: String): Option[Int] ={
    val el = jsObject.get(key)
    if(el == null || el.isJsonNull){
      None
    }else{
      Some(el.getAsInt)
    }
  }

  override def customPollStatusFailure: PartialFunction[(ExecutionHandle, Exception), ExecutionHandle] = {
    case (oldHandle: StandardAsyncPendingExecutionHandle@unchecked, e: Exception) =>
      jobLogger.error(s"$tag VK Job ${oldHandle.pendingJob.jobId} has not been found, failing call")
      FailedNonRetryableExecutionHandle(e, kvPairsToSave = None)
  }

  override def handleExecutionFailure(status: StandardAsyncRunState, returnCode: Option[Int]) = {
    status match {
      case Cancelled => Future.successful(AbortedExecutionHandle)
      case _ => super.handleExecutionFailure(status, returnCode)
    }
  }

  //  /**
  //    * Process a successful run, as defined by `isSuccess`.
  //    *
  //    * @param runStatus  The run status.
  //    * @param handle     The execution handle.
  //    * @param returnCode The return code.
  //    * @return The execution handle.
  //    */
  //  override def handleExecutionSuccess(runStatus: StandardAsyncRunState,
  //                             handle: StandardAsyncPendingExecutionHandle,
  //                             returnCode: Int)(implicit ec: ExecutionContext): Future[ExecutionHandle] = {
  //    super.handleExecutionSuccess(runStatus, handle, returnCode) map {
  //      case handle: FailedNonRetryableExecutionHandle => FailedRetryableExecutionHandle(handle.throwable)
  //      case handle: ExecutionHandle => handle
  //    }
  //  }

  override def isTerminal(runStatus: VkRunStatus): Boolean = {
    runStatus.isTerminal
  }

  override def isDone(runStatus: VkRunStatus): Boolean = {
    runStatus match {
      case Complete => true
      case _ => false
    }
  }

  def writeMetadataJson(workflowId: WorkflowId, serviceRegistryActor: ActorRef) = {
    var status = "notStart"
    val id = workflowId.id
    val resp = vkStatusManager.getMetadata(workflowId, serviceRegistryActor)
    Try(Await.ready(resp, 60 seconds)) match {
      case Success(f) => f.value.get match {
        case Success(r) =>
          val path = s"${id}/metadata.json"
          val absPath = getPath(path) match {
            case Success(absoluteOutputPath) if absoluteOutputPath.isAbsolute => absoluteOutputPath
            case _ => vkJobPaths.callExecutionRoot.resolve(path)
          }
          Await.result(asyncIo.writeAsync(absPath, r.asInstanceOf[SuccessfulMetadataJsonResponse].responseJson.toString(), Seq.empty), Duration.Inf)
        case Failure(_) =>
          status = "Failed"
      }
      case Failure(_) => // handle timeout
    }
    status.replaceAll("""[,"\s]+(|.*[^,\s])[,"\s]+""", "$1")
  }

  override def mapOutputWomFile(womFile: WomFile): WomFile = {
//    jobLogger.info(s"mapOutputWomFile start.")
//    actorSystem.scheduler.scheduleOnce(30 seconds, context.self, "wait 30s for metadata query after finished.")
//    jobLogger.info(s"mapOutputWomFile get metadata.")
//    jobLogger.info(s"mapOutputWomFile get metadata done.")
    womFile mapFile { path =>
//      val path = "/Users/dts/cromwell/cromwell/vk.conf"
      val absPath = getPath(path) match {
        case Success(absoluteOutputPath) if absoluteOutputPath.isAbsolute => absoluteOutputPath
        case _ => vkJobPaths.callExecutionRoot.resolve(path)
      }
      if (!absPath.exists) {
        throw new FileNotFoundException(s"Could not process output, file not found: ${absPath.pathAsString}")
      } else {
        syncOutput(absPath)
        absPath.pathAsString
      }
    }
  }

  private def syncOutput(path: Path) = {
    if(!vkConfiguration.storagePath.isEmpty && !path.pathAsString.startsWith("obs://")) {
      val prePath = vkJobPaths.workflowPaths.executionRoot.pathAsString
      var destPathStr = path.pathAsString
      if(path.pathAsString.startsWith(prePath)){
        destPathStr = path.pathAsString.replace(prePath, vkConfiguration.storagePath.get)
      } else {
        destPathStr = vkConfiguration.storagePath.get + path.pathAsString
      }
      val destPath = getPath(destPathStr).get
      if(vkConfiguration.async) {
        asyncIo.copyAsync(path, destPath)
      } else {
        Await.result(asyncIo.copyAsync(path, destPath), Duration.Inf)
      }
    }
  }

  private def makeRequest[A](request: HttpRequest)(implicit um: Unmarshaller[ResponseEntity, A]): Future[A] = {
    for {
      response <- withRetry(() => {
        val nJobs = vkStatusManager.jobNumber.get()
        val maxJob = vkConfiguration.maxJob
        if (nJobs > maxJob){
          val messageTooManyJobs = s"The concurrent-job-limit is ${maxJob}, current job num is ${nJobs}. So stop submission."
          return Future.failed(RateLimitException(s"Too many VK requests: ${messageTooManyJobs}"))
        }
        // increase job counter in avoid of too many jobs' creation made fetch un-work so counter not update.
        vkStatusManager.jobNumber.incrementAndGet()
        val rsp = if (vkConfiguration.region == "cn-north-7"){
          Await.result(Http().singleRequest(request, VkAsyncBackendJobExecutionActor.badCtx), Duration.Inf)
        }else{
          Await.result(Http().singleRequest(request), Duration.Inf)
        }
        if (rsp.status.isFailure() && rsp.status.intValue() == 429) {
          Future.failed(new RateLimitException(rsp.status.defaultMessage()))
        } else {
          Future.successful(rsp)
        }
      })
      data <- if (response.status.isFailure()) {
        response.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String) flatMap { errorBody =>
          Future.failed(new RuntimeException(s"Failed VK request: Code ${response.status.intValue()}, Body = $errorBody"))
        }
      } else {
        Unmarshal(response.entity).to[A]
      }
    } yield data
  }

  private def withRetry[A](work: () => Future[A]): Future[A] = {
    Retry.withRetry(work, maxRetries=Option(3), isTransient = isTransient, isFatal = isFatal, backoff = pollBackOff)(context.system)
  }

  override def isTransient(throwable: Throwable): Boolean = {
    throwable match {
      case _: RateLimitException => true
      case _ => false
    }
  }

  override def isFatal(throwable: Throwable): Boolean = throwable match {
    case _: RateLimitException => false
    case _: Error => true
    case _: RuntimeException => false
    case _: InterruptedException => true
    case _: CromwellFatalExceptionMarker => true
    case e: ExecutionException => Option(e.getCause).exists(isFatal)
    case _ => false
  }

  override def handleExecutionResult(status: StandardAsyncRunState,
                            oldHandle: StandardAsyncPendingExecutionHandle): Future[ExecutionHandle] = {

    def memoryRetryRC: Future[Boolean] = {
      def returnCodeAsBoolean(codeAsOption: Option[String]): Boolean = {
        codeAsOption match {
          case Some(codeAsString) =>
            Try(codeAsString.trim.toInt) match {
              case Success(code) => code match {
                case StderrContainsRetryKeysCode => true
                case _ => false
              }
              case Failure(e) =>
                log.error(s"'CheckingForMemoryRetry' action exited with code '$codeAsString' which couldn't be " +
                  s"converted to an Integer. Task will not be retried with double memory. Error: ${ExceptionUtils.getMessage(e)}")
                false
            }
          case None => false
        }
      }

      def readMemoryRetryRCFile(fileExists: Boolean): Future[Option[String]] = {
        if (fileExists)
          asyncIo.contentAsStringAsync(jobPaths.memoryRetryRC, None, failOnOverflow = false).map(Option(_))
        else
          Future.successful(None)
      }

      for {
        fileExists <- asyncIo.existsAsync(jobPaths.memoryRetryRC)
        retryCheckRCAsOption <- readMemoryRetryRCFile(fileExists)
        retryWithMoreMemory = returnCodeAsBoolean(retryCheckRCAsOption)
      } yield retryWithMoreMemory
    }

    val stderr = jobPaths.standardPaths.error
    val stdout = jobPaths.standardPaths.output
    val script = jobPaths.script
    lazy val stderrAsOption: Option[Path] = Option(stderr)

    val stderrSizeAndReturnCodeAndMemoryRetry = for {
      returnCodeAsString <- asyncIo.contentAsStringAsync(jobPaths.returnCode, None, failOnOverflow = false)
      // Only check stderr size if we need to, otherwise this results in a lot of unnecessary I/O that
      // may fail due to race conditions on quickly-executing jobs.
      stderrSize <- if (failOnStdErr) asyncIo.sizeAsync(stderr) else Future.successful(0L)
      retryWithMoreMemory <- memoryRetryRC
    } yield (stderrSize, returnCodeAsString, retryWithMoreMemory)

    stderrSizeAndReturnCodeAndMemoryRetry flatMap {
      case (stderrSize, returnCodeAsString, retryWithMoreMemory) =>
        val tryReturnCodeAsInt = Try(returnCodeAsString.trim.toInt)

        if (isDone(status)) {
          syncOutput(stderr)
          syncOutput(stdout)
          syncOutput(script)
          tryReturnCodeAsInt match {
            case Success(returnCodeAsInt) if failOnStdErr && stderrSize.intValue > 0 =>
              val executionHandle = Future.successful(FailedNonRetryableExecutionHandle(StderrNonEmpty(jobDescriptor.key.tag, stderrSize, stderrAsOption), Option(returnCodeAsInt), None))
              retryElseFail(executionHandle)
            case Success(returnCodeAsInt) if isAbort(returnCodeAsInt) =>
              Future.successful(AbortedExecutionHandle)
            case Success(returnCodeAsInt) if !continueOnReturnCode.continueFor(returnCodeAsInt) =>
              val executionHandle = Future.successful(FailedNonRetryableExecutionHandle(WrongReturnCode(jobDescriptor.key.tag, returnCodeAsInt, stderrAsOption), Option(returnCodeAsInt), None))
              retryElseFail(executionHandle)
            case Success(returnCodeAsInt) if retryWithMoreMemory  =>
              val executionHandle = Future.successful(FailedNonRetryableExecutionHandle(RetryWithMoreMemory(jobDescriptor.key.tag, stderrAsOption), Option(returnCodeAsInt), None))
              retryElseFail(executionHandle, retryWithMoreMemory)
            case Success(returnCodeAsInt) =>
              handleExecutionSuccess(status, oldHandle, returnCodeAsInt)
            case Failure(_) =>
              Future.successful(FailedNonRetryableExecutionHandle(ReturnCodeIsNotAnInt(jobDescriptor.key.tag, returnCodeAsString, stderrAsOption), kvPairsToSave = None))
          }
        } else {
          tryReturnCodeAsInt match {
            case Success(returnCodeAsInt) if retryWithMoreMemory =>
              val executionHandle = Future.successful(FailedNonRetryableExecutionHandle(RetryWithMoreMemory(jobDescriptor.key.tag, stderrAsOption), Option(returnCodeAsInt), None))
              retryElseFail(executionHandle, retryWithMoreMemory)
            case _ =>
              val failureStatus = handleExecutionFailure(status, tryReturnCodeAsInt.toOption)
              retryElseFail(failureStatus)
          }
        }
    } recoverWith {
      case exception =>
        if (isDone(status)) Future.successful(FailedNonRetryableExecutionHandle(exception, kvPairsToSave = None))
        else {
          val failureStatus = handleExecutionFailure(status, None)
          retryElseFail(failureStatus)
        }
    }
  }

}

private case class RateLimitException(message: String) extends RuntimeException
