package cromwell.backend.impl.vk

import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor}
import cromwell.core.path.Path
import skuber.Resource.Quantity
import skuber.Volume.{HostPath, Mount, PersistentVolumeClaimRef}
import skuber.batch.Job
import skuber.{Container, LocalObjectReference, ObjectMeta, Pod, PodSecurityContext, Resource, RestartPolicy, Volume}
import wdl4s.parser.MemoryUnit

final case class VkTask(jobDescriptor: BackendJobDescriptor,
                        configurationDescriptor: BackendConfigurationDescriptor,
                        vkPaths: VkJobPaths,
                        runtimeAttributes: VkRuntimeAttributes,
                        containerWorkDir: Path,
                        dockerImageUsed: String,
                        jobShell: String,
                        vkConfiguration: VkConfiguration) {

  private val workflowDescriptor = jobDescriptor.workflowDescriptor
  private val workflowId = workflowDescriptor.id
  private val rootWorkflowId = workflowDescriptor.rootWorkflowId
  private val fullyQualifiedTaskName = jobDescriptor.taskCall.localName.replaceAll("_", "-").toLowerCase()
  private val index = jobDescriptor.key.index.getOrElse(0)
  private val attempt = jobDescriptor.key.attempt
  val name: String = fullyQualifiedTaskName + "-" + index + "-" + attempt + "-" + workflowId.shortString

  // contains the script to be executed
  val commandScriptPath = vkPaths.callExecutionDockerRoot.resolve("script").toString

  val ram :: _ = Seq(runtimeAttributes.memory) map {
    case Some(x) =>
      Option(x.to(MemoryUnit.GB).amount)
    case None =>
      None
  }
  private var cpu = runtimeAttributes.cpu.getOrElse(ram.getOrElse(2.0)/2)
  private var memory = ram.getOrElse(cpu * 2)
  if (cpu >= 32){
    cpu = cpu- 0.5
    memory = memory -1
  }
  private val resources = if(runtimeAttributes.gpuType.isEmpty && runtimeAttributes.gpuType.isEmpty){
    Option(Resource.Requirements(
      requests = Map(
        "cpu" -> Quantity(cpu.toString),
        "memory" -> Quantity(memory.toString+"Gi"),
      ),
      limits = Map(
        "cpu" -> Quantity(cpu.toString),
        "memory" -> Quantity(memory.toString+"Gi"),
      )
    ))
  } else {
    val gpu = runtimeAttributes.gpuCount.map(_.value.toString).get
    val gpuType = runtimeAttributes.gpuType.get
    Option(Resource.Requirements(
      requests = Map(
        "cpu" -> Quantity(cpu.toString),
        "memory" -> Quantity(memory.toString+"Gi"),
        gpuType -> Quantity(gpu),
      ),
      limits = Map(
        "cpu" -> Quantity(cpu.toString),
        "memory" -> Quantity(memory.toString+"Gi"),
        gpuType -> Quantity(gpu),
      )
    ))
  }

  private val pvcStr = if (configurationDescriptor.backendConfig.hasPath("pvc")){
    configurationDescriptor.backendConfig.getString("pvc")
  }else{
    ""
  }
  private val mountPathStr = if (configurationDescriptor.backendConfig.hasPath("pvcPath")){
    configurationDescriptor.backendConfig.getString("pvcPath")
  }else{
    ""
  }
  private val pvcList = pvcStr.split(",")
  private val mountPathList = mountPathStr.split(",")
  private var mounts = List[Mount]()
  private var volumes = List[Volume]()

  private val isCCI = vkConfiguration.isCCI
  if(!pvcStr.equals("") && pvcList.length == mountPathList.length) {
    for (i <- 0 until pvcList.length){
      val pvcName = pvcList(i)
      val mountPath = mountPathList(i)
      val mountElem = Mount(
        name = pvcName,
        mountPath = mountPath
      )
      var volumeElem = Volume(
        name = pvcName,
        source = PersistentVolumeClaimRef(
          claimName = pvcList(i)
        )
      )
      if(!isCCI && pvcName.slice(0, 8).toLowerCase() == "hostpath") {
        volumeElem = Volume(
          name = pvcName,
          source = HostPath(
            path = mountPath,
            `type` = Option("")
          )
        )
      }
      mounts = mounts :+ mountElem
      volumes = volumes :+ volumeElem
    }
  }

  if(!runtimeAttributes.disks.isEmpty && isCCI){
    for(disk <- runtimeAttributes.disks.get){
      mounts = mounts :+ Mount(
        name = disk.name,
        mountPath = disk.mountPoint.pathAsString
      )
    }
  }
  private val cmdWithObsSideCar =
    s"""
       |GCS_EXIT_WRAPPER () {
       |[ -d /obssidecar/terminate ] && echo > /obssidecar/terminate/${name}
       |}
       |trap GCS_EXIT_WRAPPER exit;
       |
       |${jobShell} ${commandScriptPath}
       |""".stripMargin
  val containers = List(Container(
    name = fullyQualifiedTaskName,
    image = dockerImageUsed,
    command = List(jobShell, "-c", cmdWithObsSideCar),
    workingDir = runtimeAttributes.dockerWorkingDir,
    resources = resources,
    volumeMounts = mounts
  ))

  var imagePullSecretsName = "imagepull-secret"
  var secContext = PodSecurityContext()
  if (!isCCI) {
    val uid = Some(vkConfiguration.uid.toInt)
    val gid = Some(vkConfiguration.gid.toInt)
    val fid = Some(vkConfiguration.fid.toInt)
    secContext = PodSecurityContext(runAsUser=uid, runAsGroup=gid, fsGroup=fid)
    imagePullSecretsName = "default-secret"
  }
  val podSpec = Pod.Spec(
    containers = containers,
    volumes = if(!pvcStr.equals("")) volumes else Nil,
    restartPolicy = RestartPolicy.OnFailure,
    securityContext = Some(secContext),
    imagePullSecrets = List(LocalObjectReference(
      name = imagePullSecretsName
    ))
  )

  val wdlExecName = if(workflowDescriptor.rootWorkflow.name.length > 64){
    workflowDescriptor.rootWorkflow.name.substring(0,64);
  } else {
    workflowDescriptor.rootWorkflow.name
  }

  val labels = Map(
    "gcs-wdlexec-id" -> rootWorkflowId.toString,
    "system-tag.cci.io/gcs-wdlexec-id" -> rootWorkflowId.toString,
    "system-tag.cci.io/gcs-wdlexec-name" -> wdlExecName,
    "gcs-wdl-name" -> "cromwell",
    "gcs.task.name" -> name,
    "gcs.source.name" -> fullyQualifiedTaskName
  )
  val annotations = Map(
    "obssidecar-injector-webhook/cpu" -> "0.5",
    "obssidecar-injector-webhook/memory" -> "1Gi",
    "obssidecar-injector-webhook/inject" -> "true"
  )

  val podMetadata = ObjectMeta(name=fullyQualifiedTaskName,labels = labels, annotations = annotations)

  val templateSpec = Pod.Template.Spec(metadata=podMetadata).withPodSpec(podSpec)

  val jobMetadata = ObjectMeta(name=name,labels = labels)

  val job = Job(metadata=jobMetadata).withTemplate(templateSpec)

}
