package cromwell.backend.impl.vk;


public enum VkDiskType {
    LOCAL("LOCAL", "LocalVolume"),
    SSD("SSD", "LocalSSD"),
    HDD("HDD", "LocalVolume"),
    SATA("SATA", "LocalVolume"),
    SAS("SAS", "LocalVolume");

    public final String diskTypeName;
    public final String hwsTypeName;

    VkDiskType(final String diskTypeName, final String hwsTypeName) {
        this.diskTypeName = diskTypeName;
        this.hwsTypeName = hwsTypeName;
    }
}