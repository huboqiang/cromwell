package cromwell.backend.impl.vk

import java.util.Date
import akka.actor.{ActorContext, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import akka.http.scaladsl.model.headers.RawHeader
import akka.pattern.ask

import scala.collection.concurrent.TrieMap
import akka.util.ByteString
import com.google.gson.{JsonObject, JsonParser}
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import cromwell.core.{CromwellFatalExceptionMarker, WorkflowId}
import cromwell.core.retry.Retry.withRetry
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.services.{MetadataJsonResponse, SuccessfulMetadataJsonResponse}
import cromwell.services.keyvalue.KeyValueServiceActor.{KvAction, KvFailure, KvGet, KvKeyLookupFailed, KvPair, KvPut, KvPutSuccess, KvResponse, ScopedKey}
import cromwell.services.keyvalue.KeyValueServiceActor
import cromwell.services.metadata.MetadataService.GetSingleWorkflowMetadataAction

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}
import scala.language.postfixOps
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, ExecutionException, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}



class VkStatusManager(vkConfiguration: VkConfiguration){
  var statusMap = TrieMap[String, TrieMap[String, JsonObject]]()
  var tmpStatusMap = TrieMap[String, TrieMap[String, JsonObject]]()
  var jobNumber = new AtomicInteger(0);

  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  private val namespace = vkConfiguration.namespace
  private val apiServerUrl = vkConfiguration.k8sURL
  private val badSslConfig: AkkaSSLConfig = AkkaSSLConfig(ActorSystem.apply()).mapSettings(s =>
    s.withLoose(
      s.loose
        .withDisableHostnameVerification(true)
    )
  )
  private val badCtx = Http(ActorSystem.apply()).createClientHttpsContext(badSslConfig)
  private val data = """{"kind" : "DeleteOptions", "apiVersion" : "v1",  "propagationPolicy" : "Background" }"""
  private val entity = HttpEntity(ContentTypes.`application/json`, data.getBytes())

  val runnable = new Runnable {
    override def run() = {
      fetch()
        .onComplete {
          case Success(_) =>
          case Failure(err)   => sys.error(s"status scheduler error in this round: ${err}")
        }
    }
  }
  val service = Executors.newSingleThreadScheduledExecutor()
  service.scheduleAtFixedRate(runnable, 0, 15, TimeUnit.SECONDS)


  def submit(workflowId: String, context: ActorContext): Unit ={
    println(s"submit ${workflowId}")
  }

  def getStatus(workflowId: String, jobId: String):Option[JsonObject] = {
    val r = statusMap.get(workflowId)
    if (r == None){
      return None
    }
    val jobToJson = r.get.get(jobId)
    jobToJson
  }

  def setStatus(workflowId: String, jobId: String, jobObject: JsonObject) = {
    if (statusMap.contains(workflowId)){
      val jobToJson = statusMap(workflowId)
      jobToJson.update(jobId, jobObject)
      statusMap.update(workflowId, jobToJson)
    }else{
      val jobToJson = TrieMap(jobId -> jobObject)
      statusMap.update(workflowId, jobToJson)
    }
  }

  def setStatusTmp(workflowId: String, jobId: String, jobObject: JsonObject) = {
    if (tmpStatusMap.contains(workflowId)){
      val jobToJson = tmpStatusMap(workflowId)
      jobToJson.update(jobId, jobObject)
      tmpStatusMap.update(workflowId, jobToJson)
    }else{
      val jobToJson = TrieMap(jobId -> jobObject)
      tmpStatusMap.update(workflowId, jobToJson)
    }
  }

  def fetch()= {
    println(s"fetch ${namespace} begin at ${new Date().toString}")
    tmpStatusMap = TrieMap[String, TrieMap[String, JsonObject]]()
    makeRequest(HttpRequest(
      headers = List(RawHeader("X-Auth-Token", vkConfiguration.token.getValue())),
      uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs")) map {
      response => {
        println(s"fetch ${namespace} end at ${new Date().toString}")
        val jobList = response.get("items").getAsJsonArray
        val iterator = jobList.iterator()
        while(iterator.hasNext){
          val job = iterator.next()
          val jobMetadata = job.getAsJsonObject.get("metadata").getAsJsonObject
          if ( jobMetadata.has("name") && jobMetadata.has("labels") ){
            val jobId = jobMetadata.get("name").getAsString
            val labelObject = jobMetadata.get("labels").getAsJsonObject
            if (labelObject.has("gcs-wdlexec-id")){
                val workflowId = labelObject.get("gcs-wdlexec-id").getAsString
                setStatusTmp(workflowId, jobId, job.getAsJsonObject)
            }
          }
        }
        statusMap = tmpStatusMap
        jobNumber.set(jobList.size)
        println(s"fetch size ${jobNumber.get()}")
        updateJobQuotaLimit()
      }
    }
  }

  def remove(workflowId: WorkflowId): Unit ={
    val id = workflowId.id.toString
    println(s"delete all finished jobs' statusMap info in ${id} begin at ${new Date().toString}")
    val cancellable = statusMap.get(id)
    if (cancellable.isEmpty){
      return ()
    }

    // Let bce-apiserver delete all finished jobs so that bce can get the pod actual running time.
    //  for ( (jobId, _) <- cancellable.get ){
    //    var resp = TryDeleteVkJob(jobId)
    //    while (resp == "retry"){
    //      resp = TryDeleteVkJob(jobId)
    //    }
    //    if (resp != "notFound") {
    //      println(s"kubectl delete job ${jobId} done with status ${resp}.")
    //    }
    //  }
    statusMap.remove(id)
    println(s"delete all all finished jobs' statusMap info in ${id} end at ${new Date().toString}")
  }

  def TryDeleteVkJob(jobId: String): String = {
    val resp = deleteVkJob(jobId)
    Try(Await.ready(resp, 100 seconds)) match {
      case Success(f) => f.value.get match {
        case Success(_) =>
          "done"
        case Failure(ex) =>
          // Message likes " java.lang.RuntimeException: Failed VK request: Code ErrCode Body = $errorBody ".
          // Code 429 for keep-retrying until the req/minute level below the limit.
          val res = ex.getMessage.split(" ")(5) match{
            case "429" => "retry"
            case "404" => "notFound"
            case _ => "failed"
          }
          if (res != "notFound"){
            println(s"delete job ${jobId} failed with ${ex}.")
          }
          res
      }
      case Failure(_) =>
        "timeout"
    }
  }

  private def updateJobQuotaLimit() = {
    val resQuota = makeRequest(HttpRequest(method = HttpMethods.GET,
      headers = List(RawHeader("X-Auth-Token", vkConfiguration.token.getValue())),
      uri = s"${apiServerUrl}/api/v1/namespaces/${namespace}/resourcequotas/compute-resources",
      entity = entity))

    var nJobQuota = -1
    resQuota.onComplete {
        case Success(resp) =>
          nJobQuota = resp.get("spec").getAsJsonObject.get("hard").getAsJsonObject.get("count/jobs.batch").getAsInt
          println(s"Update concurrent-job-limit to current k8s quota: count/jobs.batch=${vkConfiguration.maxJob}")
        case Failure(err)   =>
          val errCode = err.getMessage.split("\\s+")(5)
          if (errCode != "404"){
            println(s"Quota query failed with ${err}.")
          }
      }

    if (nJobQuota > 0){
      vkConfiguration.maxJob = nJobQuota
    }
  }

  def deleteVkJob(jobId: String): Future[JsonObject] = {
    makeRequest(HttpRequest(method = HttpMethods.DELETE,
      headers = List(RawHeader("X-Auth-Token", vkConfiguration.token.getValue())),
      uri = s"${apiServerUrl}/apis/batch/v1/namespaces/${namespace}/jobs/${jobId}",
      entity = entity))
  }

  private def makeRequest(request: HttpRequest): Future[JsonObject] = {
    withRetry(() => {
      for {
        // response <- withRetry(() => Http().singleRequest(request))
        response <- if (vkConfiguration.region == "cn-north-7") {
          withRetry(() => Http().singleRequest(request, badCtx))
        } else {
          withRetry(() => Http().singleRequest(request))
        }
        data <- if (response.status.isFailure()) {
          response.entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String) flatMap { errorBody =>
            Future.failed(new RuntimeException(s"Failed VK request: Code ${response.status.intValue()} Body = $errorBody"))
          }
        } else {
          response.entity.withoutSizeLimit.toStrict(5000.millis).map[JsonObject] {
            res => {
              JsonParser.parseString(res.data.utf8String).getAsJsonObject
            }
          }
        }
      } yield data
    },isTransient = isTransient, maxRetries=Option(3), backoff=SimpleExponentialBackoff(1 seconds, 10 seconds, 2))
  }

  def isTransient(throwable: Throwable): Boolean = {
    throwable match {
      case _: RateLimitException => true
      case _ => false
    }
  }

  def isFatal(throwable: Throwable): Boolean = throwable match {
    case _: RateLimitException => false
    case _: Error => true
    case _: RuntimeException => true
    case _: InterruptedException => true
    case _: CromwellFatalExceptionMarker => true
    case e: ExecutionException => Option(e.getCause).exists(isFatal)
    case _ => false
  }

  def getMetadata(workflowId: WorkflowId, serviceRegistryActor: ActorRef): Future[MetadataJsonResponse] = {
    val readMetadataRequest = (w: WorkflowId) =>
      GetSingleWorkflowMetadataAction(w, None, None, expandSubWorkflows = true, metadataSourceOverride = None)

    val resp = serviceRegistryActor.ask(readMetadataRequest(workflowId))(600 seconds).mapTo[MetadataJsonResponse]
    resp
  }

  def getWorkflowStatus(workflowId: WorkflowId, serviceRegistryActor: ActorRef) = {
    var status = "notStart"
    val resp = getMetadata(workflowId, serviceRegistryActor)
    Try(Await.ready(resp, 60 seconds)) match {
      case Success(f) => f.value.get match {
        case Success(r) =>
          val ss = r.asInstanceOf[SuccessfulMetadataJsonResponse].responseJson
          status = ss.getFields(("status"))(0).toString()
        case Failure(_) =>
          status = "Failed"
      }
      case Failure(_) => // handle timeout
    }
    status.replaceAll("""[,"\s]+(|.*[^,\s])[,"\s]+""", "$1")
  }

}

final class InMemoryKvServiceActor extends KeyValueServiceActor {
  implicit val ec: ExecutionContext = context.dispatcher

  var kvStore = Map.empty[ScopedKey, String]

  override def receive = {
    case get: KvGet => respond(sender, get, doGet(get))
    case put: KvPut => respond(sender, put, doPut(put))
  }

  def doGet(get: KvGet): Future[KvResponse] = kvStore.get(get.key) match {
    case Some(value) => Future.successful(KvPair(get.key, value))
    case None => Future.successful(KvKeyLookupFailed(get))
  }

  def doPut(put: KvPut): Future[KvResponse] = {
    kvStore += (put.key -> put.pair.value)
    Future.successful(KvPutSuccess(put))
  }

  override protected def kvReadActorProps = Props.empty

  override protected def kvWriteActorProps = Props.empty

  private def respond(replyTo: ActorRef, action: KvAction, response: Future[KvResponse]): Unit = {
    response.onComplete {
      case Success(x) => replyTo ! x
      case Failure(ex) => replyTo ! KvFailure(action, ex)
    }
  }
}

